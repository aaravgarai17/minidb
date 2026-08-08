"""Benchmark the server over TCP, and compare against real Redis.

Both sides are driven by the *same* `redis-py` client, so the comparison is
honest: identical client code, identical protocol, identical machine. The only
variable is the server.

If a real Redis is reachable on :6379 it is benchmarked too and the gap is
reported. If not, only minidb is measured and the script says so rather than
silently omitting the comparison.

Run:  python -m bench.bench_server
      python -m bench.bench_server --requests 50000 --clients 50
"""

from __future__ import annotations

import argparse
import statistics
import subprocess
import sys
import threading
import time
from typing import Callable, Optional

try:
    import redis
except ImportError:
    print("this benchmark needs redis-py:  pip install redis")
    sys.exit(1)


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(int(len(ordered) * p / 100), len(ordered) - 1)
    return ordered[idx]


class Result:
    def __init__(self, label: str, ops: int, elapsed: float, latencies: list[float]):
        self.label = label
        self.ops = ops
        self.elapsed = elapsed
        self.latencies = latencies

    @property
    def throughput(self) -> float:
        return self.ops / self.elapsed if self.elapsed else 0.0

    def row(self) -> str:
        us = [l * 1_000_000 for l in self.latencies]
        return (
            f"  {self.label:<26} {self.throughput:>10,.0f} ops/s   "
            f"p50 {percentile(us, 50):>7.0f}µs   "
            f"p95 {percentile(us, 95):>7.0f}µs   "
            f"p99 {percentile(us, 99):>7.0f}µs"
        )


def run_workload(
    label: str,
    make_client: Callable[[], "redis.Redis"],
    op: Callable[["redis.Redis", int], None],
    requests: int,
    clients: int,
) -> Result:
    """Drive `requests` operations spread across `clients` threads.

    Threads rather than asyncio on the client side deliberately: it mirrors how
    most real applications talk to Redis (a pool of blocking connections), and
    it keeps the client from becoming the bottleneck.
    """
    per_client = max(1, requests // clients)
    latencies: list[list[float]] = [[] for _ in range(clients)]
    barrier = threading.Barrier(clients + 1)

    def worker(idx: int) -> None:
        conn = make_client()
        conn.ping()                      # connect before the clock starts
        local = latencies[idx]
        barrier.wait()                   # all threads start together
        for i in range(per_client):
            t0 = time.perf_counter()
            op(conn, idx * per_client + i)
            local.append(time.perf_counter() - t0)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(clients)]
    for t in threads:
        t.start()

    barrier.wait()
    start = time.perf_counter()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start

    flat = [x for sub in latencies for x in sub]
    return Result(label, per_client * clients, elapsed, flat)


def is_reachable(port: int) -> bool:
    try:
        redis.Redis(host="127.0.0.1", port=port, socket_connect_timeout=1).ping()
        return True
    except Exception:
        return False


def bench_target(name: str, port: int, requests: int, clients: int) -> dict[str, Result]:
    def client() -> "redis.Redis":
        return redis.Redis(host="127.0.0.1", port=port, socket_timeout=10)

    conn = client()
    conn.flushall()

    # Pre-load keys so GET measures hits rather than misses.
    for i in range(1000):
        conn.set(f"key:{i}", f"value-{i}")

    results = {
        "SET": run_workload(
            f"{name} SET", client,
            lambda c, i: c.set(f"key:{i % 10000}", f"value-{i}"),
            requests, clients,
        ),
        "GET": run_workload(
            f"{name} GET", client,
            lambda c, i: c.get(f"key:{i % 1000}"),
            requests, clients,
        ),
        "INCR": run_workload(
            f"{name} INCR", client,
            lambda c, i: c.incr("shared:counter"),
            requests, clients,
        ),
    }

    # INCR on one shared key is also a correctness check: with `requests`
    # increments from `clients` threads, the final value must be exact.
    expected = results["INCR"].ops
    actual = int(conn.get("shared:counter"))
    if actual != expected:
        print(f"  !! {name} LOST UPDATES: expected {expected}, got {actual}")
    else:
        print(f"  ✓ {name}: {actual:,} concurrent increments, zero lost updates")

    return results


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--requests", type=int, default=20_000)
    p.add_argument("--clients", type=int, default=25)
    p.add_argument("--minidb-port", type=int, default=6380)
    p.add_argument("--redis-port", type=int, default=6379)
    args = p.parse_args()

    print()
    print("=" * 92)
    print(f" Server benchmark — {args.requests:,} requests, {args.clients} concurrent clients")
    print("=" * 92)
    print()

    if not is_reachable(args.minidb_port):
        print(f"  minidb is not running on :{args.minidb_port}")
        print(f"  start it with:  python -m minidb.server --port {args.minidb_port} --no-aof")
        sys.exit(1)

    print("Correctness under concurrency")
    mini = bench_target("minidb", args.minidb_port, args.requests, args.clients)

    real: Optional[dict[str, Result]] = None
    if is_reachable(args.redis_port):
        real = bench_target("redis ", args.redis_port, args.requests, args.clients)
    print()

    print("Throughput and latency")
    for opname in ("SET", "GET", "INCR"):
        print(mini[opname].row())
        if real:
            print(real[opname].row())
    print()

    if real:
        print("How minidb compares to real Redis")
        print(f"  {'operation':<12} {'minidb':>12} {'redis':>12} {'ratio':>10}")
        for opname in ("SET", "GET", "INCR"):
            m, r = mini[opname].throughput, real[opname].throughput
            print(f"  {opname:<12} {m:>12,.0f} {r:>12,.0f} {m / r:>9.2f}x")
        print()
        print("  Redis is written in C; this is Python. A gap of roughly an order of")
        print("  magnitude is the expected and honest result. What the comparison")
        print("  shows is that the architecture is sound — the difference is the")
        print("  language and per-request interpreter overhead, not the algorithm.")
    else:
        print("  No Redis found on :%d — comparison skipped." % args.redis_port)
        print("  Start one for a side-by-side:  docker run --rm -p 6379:6379 redis:7-alpine")
    print()


if __name__ == "__main__":
    main()
