"""Command dispatch.

Kept deliberately separate from the networking layer so every command can be
tested by calling a function with a list of byte arguments — no sockets, no
event loop, no fixtures. `server.py` handles bytes on the wire; this module
turns an argument list into a reply.

Argument and reply conventions follow Redis closely enough that `redis-cli`
and `redis-py` behave exactly as a user would expect.
"""

from __future__ import annotations

import time
from typing import Callable, Optional

from minidb import protocol
from minidb.store import Store

# Populated by the @command decorator below.
COMMANDS: dict[bytes, "CommandSpec"] = {}


class CommandSpec:
    __slots__ = ("name", "handler", "arity", "is_write")

    def __init__(self, name: bytes, handler: Callable, arity: int, is_write: bool):
        self.name = name
        self.handler = handler
        self.arity = arity          # negative means "at least abs(arity)"
        self.is_write = is_write


def command(name: str, arity: int, is_write: bool = False):
    """Register a command.

    `arity` counts the command name itself, matching Redis's convention.
    A negative value means "at least this many".
    """
    def wrap(fn):
        COMMANDS[name.encode().upper()] = CommandSpec(
            name.encode().upper(), fn, arity, is_write
        )
        return fn
    return wrap


def _wrong_args(name: bytes) -> bytes:
    return protocol.encode_error(
        f"ERR wrong number of arguments for '{name.decode().lower()}' command"
    )


def _parse_int(raw: bytes) -> Optional[int]:
    try:
        return int(raw)
    except (ValueError, TypeError):
        return None


# ----------------------------------------------------------------- connection


@command("PING", -1)
def cmd_ping(store: Store, args: list[bytes]) -> bytes:
    if len(args) == 1:
        return protocol.encode_simple("PONG")
    return protocol.encode_bulk(args[1])


@command("ECHO", 2)
def cmd_echo(store: Store, args: list[bytes]) -> bytes:
    return protocol.encode_bulk(args[1])


@command("COMMAND", -1)
def cmd_command(store: Store, args: list[bytes]) -> bytes:
    """Stub for `COMMAND DOCS`, which redis-cli sends on connect.

    Returning an empty array is enough to satisfy it. Without this, redis-cli
    prints an error banner before every session — cosmetic, but it makes the
    server look broken in exactly the demo people will try first.
    """
    return protocol.encode_array([])


@command("HELLO", -1)
def cmd_hello(store: Store, args: list[bytes]) -> bytes:
    """Reject the RESP3 handshake so clients fall back to RESP2.

    Answering with an error is the documented way to signal "RESP2 only", and
    redis-cli handles it silently.
    """
    return protocol.encode_error(
        "NOPROTO unsupported protocol version (minidb speaks RESP2)"
    )


@command("SELECT", 2)
def cmd_select(store: Store, args: list[bytes]) -> bytes:
    """Single database only; accept SELECT 0 and reject anything else."""
    index = _parse_int(args[1])
    if index is None:
        return protocol.encode_error("ERR value is not an integer or out of range")
    if index != 0:
        return protocol.encode_error("ERR DB index is out of range")
    return protocol.encode_simple("OK")


# --------------------------------------------------------------- string ops


@command("SET", -3, is_write=True)
def cmd_set(store: Store, args: list[bytes]) -> bytes:
    """SET key value [EX seconds | PX milliseconds] [NX | XX]"""
    key, value = args[1], args[2]

    ttl: Optional[float] = None
    nx = xx = False

    i = 3
    while i < len(args):
        opt = args[i].upper()

        if opt in (b"EX", b"PX"):
            if i + 1 >= len(args):
                return protocol.encode_error("ERR syntax error")
            amount = _parse_int(args[i + 1])
            if amount is None:
                return protocol.encode_error(
                    "ERR value is not an integer or out of range"
                )
            if amount <= 0:
                return protocol.encode_error(
                    "ERR invalid expire time in 'set' command"
                )
            ttl = amount if opt == b"EX" else amount / 1000.0
            i += 2
        elif opt == b"NX":
            nx = True
            i += 1
        elif opt == b"XX":
            xx = True
            i += 1
        else:
            return protocol.encode_error("ERR syntax error")

    if nx and xx:
        return protocol.encode_error("ERR syntax error")

    exists = store.exists(key)
    if (nx and exists) or (xx and not exists):
        return protocol.encode_bulk(None)     # null reply: condition not met

    store.set(key, value, ttl=ttl)
    return protocol.encode_simple("OK")


@command("SETEX", 4, is_write=True)
def cmd_setex(store: Store, args: list[bytes]) -> bytes:
    seconds = _parse_int(args[2])
    if seconds is None:
        return protocol.encode_error("ERR value is not an integer or out of range")
    if seconds <= 0:
        return protocol.encode_error("ERR invalid expire time in 'setex' command")

    store.set(args[1], args[3], ttl=seconds)
    return protocol.encode_simple("OK")


@command("GET", 2)
def cmd_get(store: Store, args: list[bytes]) -> bytes:
    return protocol.encode_bulk(store.get(args[1]))


@command("MGET", -2)
def cmd_mget(store: Store, args: list[bytes]) -> bytes:
    return protocol.encode_array([store.get(k) for k in args[1:]])


@command("MSET", -3, is_write=True)
def cmd_mset(store: Store, args: list[bytes]) -> bytes:
    pairs = args[1:]
    if len(pairs) % 2 != 0:
        return protocol.encode_error("ERR wrong number of arguments for 'mset' command")

    for i in range(0, len(pairs), 2):
        store.set(pairs[i], pairs[i + 1])
    return protocol.encode_simple("OK")


# ------------------------------------------------------------------ counters
#
# INCR is the textbook read-modify-write: fetch, parse, add, store. In a
# threaded server this is precisely where a race lives — two threads read 5,
# both write 6, and one increment vanishes. Here the whole sequence runs inside
# a single event-loop turn with no `await`, so it is atomic by construction and
# needs no lock. `test_concurrent_incr_loses_no_updates` proves it.


def _incr_by(store: Store, key: bytes, delta: int) -> bytes:
    current = store.get(key)

    if current is None:
        value = 0
    else:
        try:
            value = int(current)
        except (ValueError, TypeError):
            return protocol.encode_error(
                "ERR value is not an integer or out of range"
            )

    value += delta
    # Preserve any existing TTL: INCR changes the value, not the lifetime.
    ttl = store.ttl(key)
    store.set(key, str(value).encode(), ttl=ttl if ttl > 0 else None)
    return protocol.encode_integer(value)


@command("INCR", 2, is_write=True)
def cmd_incr(store: Store, args: list[bytes]) -> bytes:
    return _incr_by(store, args[1], 1)


@command("DECR", 2, is_write=True)
def cmd_decr(store: Store, args: list[bytes]) -> bytes:
    return _incr_by(store, args[1], -1)


@command("INCRBY", 3, is_write=True)
def cmd_incrby(store: Store, args: list[bytes]) -> bytes:
    delta = _parse_int(args[2])
    if delta is None:
        return protocol.encode_error("ERR value is not an integer or out of range")
    return _incr_by(store, args[1], delta)


@command("DECRBY", 3, is_write=True)
def cmd_decrby(store: Store, args: list[bytes]) -> bytes:
    delta = _parse_int(args[2])
    if delta is None:
        return protocol.encode_error("ERR value is not an integer or out of range")
    return _incr_by(store, args[1], -delta)


@command("DEL", -2, is_write=True)
def cmd_del(store: Store, args: list[bytes]) -> bytes:
    return protocol.encode_integer(sum(1 for k in args[1:] if store.delete(k)))


@command("EXISTS", -2)
def cmd_exists(store: Store, args: list[bytes]) -> bytes:
    return protocol.encode_integer(sum(1 for k in args[1:] if store.exists(k)))


# ------------------------------------------------------------------- expiry


@command("TTL", 2)
def cmd_ttl(store: Store, args: list[bytes]) -> bytes:
    return protocol.encode_integer(store.ttl(args[1]))


@command("EXPIRE", 3, is_write=True)
def cmd_expire(store: Store, args: list[bytes]) -> bytes:
    seconds = _parse_int(args[2])
    if seconds is None:
        return protocol.encode_error("ERR value is not an integer or out of range")
    return protocol.encode_integer(1 if store.expire(args[1], seconds) else 0)


@command("PERSIST", 2, is_write=True)
def cmd_persist(store: Store, args: list[bytes]) -> bytes:
    return protocol.encode_integer(1 if store.persist(args[1]) else 0)


# ---------------------------------------------------------------- keyspace


@command("KEYS", 2)
def cmd_keys(store: Store, args: list[bytes]) -> bytes:
    pattern = args[1].decode("utf-8", errors="replace")
    return protocol.encode_array(store.keys(pattern))


@command("DBSIZE", 1)
def cmd_dbsize(store: Store, args: list[bytes]) -> bytes:
    return protocol.encode_integer(len(store))


@command("FLUSHALL", -1, is_write=True)
def cmd_flushall(store: Store, args: list[bytes]) -> bytes:
    store.flush()
    return protocol.encode_simple("OK")


# ------------------------------------------------------------- introspection


@command("INFO", -1)
def cmd_info(store: Store, args: list[bytes]) -> bytes:
    """Human-readable stats, in the `field:value` shape Redis uses."""
    s = store.stats()
    lines = [
        "# Server",
        "minidb_version:0.1.0",
        f"uptime_in_seconds:{int(time.time() - _START_TIME)}",
        "",
        "# Keyspace",
        f"keys:{s['size']}",
        f"capacity:{s['capacity']}",
        f"keys_with_ttl:{s['keys_with_ttl']}",
        "",
        "# Stats",
        f"keyspace_hits:{s['hits']}",
        f"keyspace_misses:{s['misses']}",
        f"hit_rate_pct:{s['hit_rate_pct']}",
        f"evicted_keys:{s['evictions']}",
        f"expired_keys:{s['expired_total']}",
    ]
    return protocol.encode_bulk("\r\n".join(lines) + "\r\n")


_START_TIME = time.time()


# ------------------------------------------------------------------ dispatch


def dispatch(store: Store, args: list[bytes]) -> tuple[bytes, bool]:
    """Execute one command.

    Returns `(reply_bytes, should_persist)`. The caller decides what to do with
    the persistence flag — the AOF is the server's concern, not this module's,
    which keeps command handlers pure and trivially testable.
    """
    if not args:
        return b"", False

    name = args[0].upper()
    spec = COMMANDS.get(name)

    if spec is None:
        readable = args[0].decode("utf-8", errors="replace")
        return (
            protocol.encode_error(f"ERR unknown command '{readable}'"),
            False,
        )

    if spec.arity >= 0 and len(args) != spec.arity:
        return _wrong_args(name), False
    if spec.arity < 0 and len(args) < -spec.arity:
        return _wrong_args(name), False

    reply = spec.handler(store, args)

    # Only persist writes that actually succeeded. Logging a failed command
    # would corrupt state on replay — e.g. a `SET k v NX` that was rejected
    # because the key existed must not be replayed as though it had applied.
    persist = spec.is_write and not reply.startswith(b"-")
    if persist and name == b"SET" and reply == b"$-1\r\n":
        persist = False               # NX/XX condition not met; nothing changed

    return reply, persist
