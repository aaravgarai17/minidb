# minidb

[![CI](https://github.com/aaravgarai17/minidb/actions/workflows/ci.yml/badge.svg)](https://github.com/aaravgarai17/minidb/actions/workflows/ci.yml)
![Coverage](https://img.shields.io/badge/coverage-76%25-green)
![Python](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12-blue)
![License](https://img.shields.io/badge/license-MIT-green)

A Redis-style in-memory key-value store built from scratch: a TCP server
speaking Redis's own wire protocol, an **O(1) LRU cache**, **TTL expiry**, and
**append-only persistence** that survives restarts.

Because it implements RESP, **the real `redis-cli` and the official `redis-py`
client work against it unmodified**:

```console
$ redis-cli -p 6380
127.0.0.1:6380> SET greeting "hello"
OK
127.0.0.1:6380> INCR counter
(integer) 1
127.0.0.1:6380> KEYS *
1) "greeting"
2) "counter"
```

---

## Verify it works (one command)

> **Common tasks:** `make help` lists everything — `make install`, `make test`,
> `make verify`, and per-project shortcuts.

```bash
./verify.sh
```

Starts the server, checks every claim below independently, and shuts down:

```
 ✓ all tests passed
 ✓ server is up on :6390
 ✓ SET works
 ✓ GET returns the value
 ✓ missing key returns nil
 ✓ DEL removes the key
 ✓ the official redis-py client drives it correctly
 ✓ the real redis-cli connects and works
 ✓ TTL reports time remaining (100 s)
 ✓ active expiry reclaimed a key nobody read
 ✓ 2000 concurrent increments from 20 clients, zero lost (2000)
 ✓ server shut down cleanly
 ✓ value restored from the append-only file
 ✓ the DEL was replayed too (not just the writes)

 Results: 14 passed, 0 failed
VERIFIED — every README claim checks out.
```

## Architecture

```
                        TCP :6380
                            │
                  ┌─────────┴─────────┐
                  │   asyncio server  │   one coroutine per connection,
                  │  (single loop)    │   single-threaded, no locks
                  └─────────┬─────────┘
                            │
                  ┌─────────┴─────────┐
                  │  RESP parser      │   Redis wire protocol
                  └─────────┬─────────┘
                            │
                  ┌─────────┴─────────┐
                  │ command dispatch  │──────────────┐
                  └─────────┬─────────┘              │ writes only
                            │                        ▼
                  ┌─────────┴─────────┐      ┌───────────────┐
                  │      Store        │      │  AOF writer   │
                  │  ┌─────────────┐  │      │  (buffered,   │
                  │  │  LRU cache  │  │      │   fsync per   │
                  │  │ dict + list │  │      │   policy)     │
                  │  └─────────────┘  │      └───────┬───────┘
                  │  expiry table     │              │
                  └─────────┬─────────┘              ▼
                            │                  minidb.aof
             background ────┤                  (replayed on start)
             expiry sweep ──┘
```

## Commands

| Category | Commands |
| -------- | -------- |
| Strings | `SET key value [EX s\|PX ms] [NX\|XX]`, `GET`, `MSET`, `MGET`, `SETEX`, `DEL`, `EXISTS` |
| Counters | `INCR`, `DECR`, `INCRBY`, `DECRBY` |
| Expiry | `TTL`, `EXPIRE`, `PERSIST` |
| Keyspace | `KEYS pattern`, `DBSIZE`, `FLUSHALL` |
| Server | `PING`, `ECHO`, `INFO`, `SELECT 0` |

---

## Design decisions

### The LRU: hash map + doubly linked list

An LRU cache must answer two questions fast: *what is the value for key K* and
*which key was used least recently*. No single structure does both — a dict has
O(1) lookup but no ordering; a list has ordering but O(n) lookup.

Running them together solves it. **The dict's values are the list's nodes**, so
finding a key by hash also hands you a pointer into the middle of the list.
Splicing that node to the front is four pointer assignments. Every operation —
`GET`, `SET`, `DEL`, eviction — is O(1).

```
      map:  {"a": ●, "b": ●, "c": ●}
                    │       │      │
     list:  HEAD ⇄ [c] ⇄  [b] ⇄  [a] ⇄ TAIL
            (sentinel)                  (sentinel)
                    ▲                │
              most recent      next to be evicted
```

Two details that matter:

- **Doubly** linked, because removing a node means rewiring its predecessor,
  and a singly linked list can only find that by scanning — O(n), exactly what
  we're avoiding.
- **Sentinel** head and tail nodes hold no data and are never removed. Their
  only job is to guarantee every real node has neighbours on both sides, which
  eliminates every "is this the first/last/only node?" branch from insert and
  remove.

`tests/test_lru.py` fuzzes 4,000 mixed operations and asserts after each one
that the dict and list still agree — the invariant that catches this
structure's signature bug, where a node is dropped from one half but not the
other.

### Concurrency: one thread, no locks

The server runs a single asyncio event loop. Each connection gets a coroutine,
but only one runs at a time and control only transfers at an `await`. No
command handler contains an `await`, so **every command runs start to finish
without interruption** — atomic by construction.

That means no mutexes, no lock contention, and none of the bugs that come with
shared mutable state across threads.

This isn't a Python workaround; **real Redis is single-threaded for the same
reason**. An in-memory store spends nearly all its time in I/O, not
computation: a `GET` is a hash lookup measured in nanoseconds, while reading the
request off a socket takes microseconds. Threads would add locking overhead to
protect a structure that was never the bottleneck.

The honest cost is that one instance uses one core. Redis accepts this too; the
standard answer is to run more instances and shard the keyspace, not to add
threads.

**The proof:** 20 clients each issue 100 `INCR` on one shared key. `INCR` is
read-modify-write — precisely where a threaded server loses updates. The result
is exactly 2000, every time (`verify.sh` step 5, and
`test_concurrent_incr_loses_no_updates`).

### TTL: why expiry is checked two different ways

**Lazy expiry** runs on read: before returning a value, check whether it has
expired. Cheap and exactly correct — an expired key is never visible to a
client. But a key written with a TTL and never read again sits in memory
forever, because nothing looks at it.

**Active expiry** fixes the leak with a background sweep. Scanning every key
would be O(n) per tick and would stall a large database, so this uses Redis's
sampling approach: take 20 random keys *that have TTLs*, delete the expired
ones, and repeat only while more than 25% of a sample turns out to be expired.
Busy when lots is expiring, nearly free when not.

Neither is sufficient alone — lazy leaks, active can briefly serve a stale
value — so both run.

### Persistence: AOF over snapshots

A snapshot writes the whole dataset periodically: compact and fast to load, but
a crash loses everything since the last one. An **append-only file** logs each
write command as it happens: larger and slower to load, but the loss window is
one command instead of several minutes. Since the point of adding persistence
was "don't lose acknowledged writes", AOF is the right trade.

Commands are stored **in RESP format — the same bytes the client sent**. Replay
therefore reuses the live command path rather than a parallel deserializer that
could drift out of sync, and the file is readable with `cat`.

**The durability knob.** `write()` only reaches the OS page cache; `fsync()`
puts bytes on the disk. So:

| `--fsync` | Guarantee | Cost |
| --------- | --------- | ---- |
| `always` | No acknowledged write is ever lost | Slowest — capped by the disk's sync rate |
| `everysec` *(default)* | Up to 1 second lost on power failure | Near zero |
| `no` | OS decides; up to ~30s exposure | Fastest |

Worth separating two failures: if only the *process* dies, data already handed
to `write()` survives, because the page cache belongs to the kernel. `fsync`
protects against the machine losing power, not against a Python exception.

A clean shutdown always fsyncs, so `everysec` still loses nothing on a graceful
stop.

**Compaction.** The log records history, not state: a key written a thousand
times leaves a thousand entries, of which only the last matters. When the file
outgrows its baseline it is rewritten as one `SET` per live key — to a temp
file, then atomically renamed, so a crash mid-rewrite leaves the original
intact.

### Protocol: RESP, not something custom

A line-based protocol would have been marginally simpler. Implementing RESP
means every existing Redis tool works unchanged — `redis-cli` connects,
`redis-py` drives it, `redis-benchmark` measures it. That converts "trust me"
into something anyone can check in ten seconds with software they already have,
and it makes the benchmark against real Redis a genuine like-for-like
comparison since both are driven by the identical client.

Bulk strings are length-prefixed, so values may contain CRLF, NUL, or arbitrary
binary with no escaping. Inline commands (`GET foo\r\n`) are also accepted, so
the server can be debugged with `netcat`.

---

## Benchmarks

Measured on an M-series MacBook Air. Reproduce with `python -m bench.bench_lru`
and `python -m bench.bench_server`.

### The data structure alone

| Operation | Throughput |
| --------- | ---------- |
| `get` (hit) | **2,667,351 ops/sec** |
| `get` (miss) | 8,073,143 ops/sec |
| `put` (no eviction) | 1,924,314 ops/sec |
| `put` (evicting every time) | 1,660,372 ops/sec |
| mixed 75% get / 25% put | 2,885,868 ops/sec |

Misses are fastest because a miss is a single failed hash lookup — no list
splice, no pointer rewiring.

### Is it really O(1)?

Throughput while the cache grows 1000×:

| Entries | Throughput |
| ------- | ---------- |
| 1,000 | 2,746,831 ops/sec |
| 10,000 | 2,641,409 ops/sec |
| 100,000 | 2,455,107 ops/sec |
| 1,000,000 | 1,564,354 ops/sec |

**A 1000× size increase costs 1.76× throughput.** An O(n) implementation would
be roughly 1000× slower — a naive LRU scanning a list to find the oldest entry
would fall off a cliff somewhere between these rows.

The residual decline is **CPU cache locality**, not algorithmic. At a million
entries the nodes no longer fit in L2/L3, so pointer chasing starts reaching
main memory. Being able to tell those two causes apart is the point of running
the sweep rather than asserting the complexity.

### Over the network

25 concurrent clients, 20,000 requests:

| Operation | Throughput | p50 | p95 | p99 |
| --------- | ---------- | --- | --- | --- |
| SET | 16,520 ops/sec | 1.2ms | 3.6ms | 5.0ms |
| GET | 17,028 ops/sec | 1.2ms | 3.5ms | 4.9ms |
| INCR | 15,923 ops/sec | 1.2ms | 3.7ms | 5.4ms |

**The most informative number here is the gap.** The data structure does 2.6M
ops/sec; the server does 17K — about 150× less. Everything in between is
syscalls, TCP, and protocol parsing. The algorithm is nowhere near the
bottleneck, which is exactly why single-threading costs nothing.

### Against real Redis

```bash
docker run --rm -d -p 6379:6379 redis:7-alpine
python -m bench.bench_server          # benchmarks both, prints the ratio
```

Redis is C; this is Python. Expect roughly an order of magnitude, and the
benchmark prints the honest ratio rather than hiding it. The point of the
comparison isn't to win — it's to show the architecture is sound and locate the
difference in the language, not the design.

---

## Running it

**Requires:** Python 3.10+. Optionally Docker, and `redis-tools` for `redis-cli`.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python -m minidb.server                     # :6380, AOF at data/minidb.aof
```

Options:

```bash
python -m minidb.server --port 6380 \
                        --max-keys 100000 \
                        --aof data/minidb.aof \
                        --fsync everysec      # always | everysec | no
python -m minidb.server --no-aof              # pure in-memory
```

Talk to it:

```bash
python -m minidb.cli                    # interactive, with history
python -m minidb.cli GET mykey          # one-shot
redis-cli -p 6380                       # the real thing also works
```

With Docker:

```bash
docker compose up --build               # minidb on :6380, real redis on :6379
```

## Tests

```bash
pytest -q                               # 158 tests
pytest --cov=minidb --cov-report=term-missing
```

Covers the LRU (including a 4,000-operation consistency fuzz and an empirical
O(1) check), TTL semantics against an injected clock, RESP encoding and
parsing, every command, and end-to-end server behaviour over real sockets:
concurrency, pipelining, eviction, graceful shutdown, and data surviving a
restart.

CI additionally runs the suite on Python 3.10/3.11/3.12, drives the server with
**real `redis-cli` and `redis-py`**, and verifies durability across an actual
process restart.

---

## What doesn't work well

Being straight about the limits, because they're the interesting part:

- **Single core.** One instance uses one CPU. Sharding across instances is the
  answer, and it isn't implemented here.
- **No replication.** A single node is a single point of failure. This is the
  natural next feature: a replica connecting, receiving a snapshot, then
  streaming subsequent writes.
- **`KEYS` is O(n)** and materialises the whole list, exactly like Redis's. Fine
  for debugging, bad on a large live database. `SCAN` with a cursor is the fix.
- **Strings only.** No lists, sets, hashes, or sorted sets.
- **No `WATCH`/`MULTI`.** Individual commands are atomic; multi-command
  transactions aren't supported.
- **Memory is bounded by key count, not bytes.** `--max-keys` counts entries, so
  a million tiny keys and a million large ones are treated alike. Real Redis
  tracks actual memory.
- **Active expiry is probabilistic.** With very many TTL'd keys, some may linger
  slightly past expiry — though never visibly, since reads check lazily.
- **Python.** Roughly an order of magnitude slower than C Redis, as benchmarked.

## Layout

```
minidb/
├── minidb/
│   ├── lru.py         # O(1) LRU: dict + doubly linked list
│   ├── store.py       # TTL, eviction, lazy + active expiry
│   ├── protocol.py    # RESP encode/decode
│   ├── commands.py    # command dispatch table
│   ├── server.py      # asyncio TCP server, graceful shutdown
│   ├── aof.py         # append-only persistence, replay, compaction
│   ├── cli.py         # interactive client
│   └── config.py
├── tests/             # 158 tests, no external services needed
├── bench/
│   ├── bench_lru.py   # data structure in isolation
│   └── bench_server.py# over TCP, and vs real Redis
├── verify.sh          # one-command proof it all works
├── Dockerfile
└── docker-compose.yml
```
