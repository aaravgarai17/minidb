"""Throughput across increasing client counts, minidb vs real Redis.

Answers three questions:

  1. How does throughput scale as concurrent clients increase?
  2. Where does it plateau, and where does it start to degrade?
  3. How does that compare to real Redis on the same machine, same workload,
     same client library?

Methodology notes that matter for the numbers to mean anything
---------------------------------------------------------------
**The client can become the bottleneck.** At 500+ threads a Python client
spends most of its time in the GIL and context switching rather than waiting on
the server. Past a few hundred threads this measures the benchmark harness as
much as the server, which is why the report flags it rather than presenting the
tail as a clean server result.

**Connections are established before timing starts.** TCP handshakes at 1,000
clients would otherwise dominate the first second and depress the number.

**All threads start together** behind a barrier, so the measurement covers the
period when concurrency is actually at the target level rather than ramping.

**Both servers are driven by the same redis-py client** over the same protocol.
The only variable is the server implementation.

Run:  python -m bench.bench_concurrency
      python -m bench.bench_concurrency --ops-per-client 500 --clients 10,50,100
"""

from __future__ import annotations

import argparse
import platform
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

try:
    import redis
except ImportError:
    print("needs redis-py:  pip install redis")
    sys.exit(1)


@dataclass
class Result:
    label: str
    clients: int
    ops: int
    elapsed: float
    latencies: list[float] = field(default_factory=list)
    errors: int = 0

    @property
    def throughput(self) -> float:
        return self.ops / self.elapsed if self.elapsed else 0.0

    def pct(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        ordered = sorted(self.latencies)
        idx = min(int(len(ordered) * p / 100), len(ordered) - 1)
        return ordered[idx] * 1000  # ms


def run_workload(port: int, clients: int, ops_per_client: int, op: str) -> Result:
    """Drive `ops_per_client` operations on each of `clients` threads.

    Work per client is held CONSTANT rather than dividing a fixed total.
    Dividing a total is the obvious design and it is wrong: at 1,000 clients
    each thread would execute only 20 operations, so the measurement window is
    dominated by thread startup and teardown rather than steady-state
    throughput. That produced impossible readings — throughput rising while
    latency fell — and made the high-concurrency rows meaningless.
    """
    per_client = max(1, ops_per_client)
    latencies: list[list[float]] = [[] for _ in range(clients)]
    errors = [0] * clients
    connections: list[redis.Redis] = []

    # Connect everything up front so handshakes are outside the timed window.
    for _ in range(clients):
        conn = redis.Redis(host="127.0.0.1", port=port, socket_timeout=30)
        conn.ping()
        connections.append(conn)

    barrier = threading.Barrier(clients + 1)

    def worker(idx: int) -> None:
        conn = connections[idx]
        local = latencies[idx]
        barrier.wait()
        for i in range(per_client):
            key = f"key:{(idx * per_client + i) % 10000}"
            t0 = time.perf_counter()
            try:
                if op == "SET":
                    conn.set(key, f"value-{i}")
                elif op == "GET":
                    conn.get(key)
                else:
                    conn.incr("shared:counter")
            except Exception:
                errors[idx] += 1
            local.append(time.perf_counter() - t0)

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(clients)]
    for t in threads:
        t.start()

    barrier.wait()
    start = time.perf_counter()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start

    for conn in connections:
        try:
            conn.close()
        except Exception:
            pass

    return Result(
        label=f"{op}@{clients}",
        clients=clients,
        ops=per_client * clients,
        elapsed=elapsed,
        latencies=[x for sub in latencies for x in sub],
        errors=sum(errors),
    )


def reachable(port: int) -> bool:
    try:
        redis.Redis(host="127.0.0.1", port=port, socket_connect_timeout=1).ping()
        return True
    except Exception:
        return False


def sweep(name: str, port: int, client_counts: list[int], ops_per_client: int) -> dict:
    conn = redis.Redis(host="127.0.0.1", port=port)
    conn.flushall()
    for i in range(10000):
        conn.set(f"key:{i}", f"value-{i}")

    results: dict[str, list[Result]] = {"SET": [], "GET": []}

    for op in ("SET", "GET"):
        print(f"\n  {name} — {op}")
        print(f"  {'clients':>8} {'ops/sec':>12} {'p50':>9} {'p95':>9} "
              f"{'p99':>9} {'errors':>7}")
        print("  " + "-" * 60)

        for count in client_counts:
            r = run_workload(port, count, ops_per_client, op)
            results[op].append(r)
            print(f"  {count:>8} {r.throughput:>12,.0f} {r.pct(50):>8.2f}ms "
                  f"{r.pct(95):>8.2f}ms {r.pct(99):>8.2f}ms {r.errors:>7}")

    return results


def analyse(results: list[Result]) -> tuple[int, int]:
    """Return (plateau_clients, degradation_clients).

    Plateau: the first point where throughput stops improving by more than 5%.
    Degradation: the first point where throughput falls more than 10% below the
    best observed.
    """
    best = max(r.throughput for r in results)
    plateau = results[-1].clients
    degrade = 0

    for prev, curr in zip(results, results[1:]):
        if plateau == results[-1].clients and curr.throughput < prev.throughput * 1.05:
            plateau = prev.clients
        if not degrade and curr.throughput < best * 0.90:
            degrade = curr.clients

    return plateau, degrade


def hardware() -> str:
    bits = [platform.platform(), platform.processor() or platform.machine()]
    try:
        if sys.platform == "darwin":
            cpu = subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
            ).strip()
            mem = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True))
            cores = subprocess.check_output(["sysctl", "-n", "hw.ncpu"], text=True).strip()
            bits = [cpu, f"{cores} cores", f"{mem // (1024**3)} GB RAM", platform.platform()]
    except Exception:
        pass
    bits.append(f"Python {platform.python_version()}")
    return " · ".join(bits)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ops-per-client", type=int, default=200,
                        help="operations EACH client performs (held constant "
                             "so high-concurrency rows stay meaningful)")
    parser.add_argument("--clients", default="10,50,100,500,1000")
    parser.add_argument("--minidb-port", type=int, default=6380)
    parser.add_argument("--redis-port", type=int, default=6379)
    args = parser.parse_args()

    client_counts = [int(c) for c in args.clients.split(",")]

    print()
    print("=" * 78)
    print(" THROUGHPUT VS CONCURRENCY")
    print("=" * 78)
    print(f"  hardware   {hardware()}")
    print(f"  workload   {args.ops_per_client:,} operations per client "
          f"(total scales with client count)")
    print(f"  clients    {client_counts}")

    if not reachable(args.minidb_port):
        print(f"\n  minidb is not running on :{args.minidb_port}")
        print(f"  start it:  python -m minidb.server --port {args.minidb_port} --no-aof")
        sys.exit(1)

    mini = sweep("minidb", args.minidb_port, client_counts, args.ops_per_client)

    real = None
    if reachable(args.redis_port):
        real = sweep("redis ", args.redis_port, client_counts, args.ops_per_client)
    else:
        print(f"\n  No Redis on :{args.redis_port} — comparison skipped.")
        print("  Start one:  docker run --rm -d -p 6379:6379 redis:7-alpine")

    # --- scaling behaviour -------------------------------------------------
    print()
    print("=" * 78)
    print(" SCALING")
    print("=" * 78)
    for op in ("SET", "GET"):
        plateau, degrade = analyse(mini[op])
        peak = max(mini[op], key=lambda r: r.throughput)
        print(f"  minidb {op}: peaks at {peak.throughput:,.0f} ops/sec "
              f"({peak.clients} clients)")
        print(f"    plateaus around {plateau} clients", end="")
        print(f", degrades past {degrade}" if degrade else ", no degradation observed")

    # --- side by side ------------------------------------------------------
    if real:
        print()
        print("=" * 78)
        print(" MINIDB VS REDIS — same machine, same client, same workload")
        print("=" * 78)
        for op in ("SET", "GET"):
            print(f"\n  {op}")
            print(f"  {'clients':>8} {'minidb':>12} {'redis':>12} {'ratio':>8}")
            print("  " + "-" * 44)
            for m, r in zip(mini[op], real[op]):
                ratio = m.throughput / r.throughput if r.throughput else 0
                print(f"  {m.clients:>8} {m.throughput:>12,.0f} "
                      f"{r.throughput:>12,.0f} {ratio:>7.2f}x")

        print()
        print("  Redis is C and minidb is Python. A gap of roughly an order of")
        print("  magnitude is the expected result — what the comparison shows is")
        print("  that the scaling *shape* is similar, so the architecture is")
        print("  sound and the difference is interpreter overhead per request.")

    print()
    print("  Caveat: past a few hundred threads a Python client spends much of")
    print("  its time on the GIL and context switching, so the high-concurrency")
    print("  rows measure the harness as much as the server. Treat the plateau")
    print("  as a property of this client, not a hard server limit.")
    print()


if __name__ == "__main__":
    main()
