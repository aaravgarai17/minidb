"""Benchmark the LRU data structure in isolation (no network, no protocol).

Separating this from the server benchmark matters: it establishes the ceiling.
If the structure itself does 3M ops/sec but the server only does 40K, the
bottleneck is unambiguously I/O and protocol parsing, not the algorithm — and
you can say so with evidence rather than assumption.

Run:  python -m bench.bench_lru
"""

import random
import statistics
import time

from minidb.lru import LRUCache


def bench(label: str, fn, ops: int, repeats: int = 5) -> None:
    """Run `fn` a few times and report the best throughput.

    Best-of rather than mean: we want the structure's capability, and any
    slower run reflects OS scheduling noise, not the code.
    """
    rates = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        elapsed = time.perf_counter() - start
        rates.append(ops / elapsed)

    best = max(rates)
    median = statistics.median(rates)
    print(f"  {label:<38} {best:>12,.0f} ops/sec   (median {median:>10,.0f})")


def main() -> None:
    random.seed(42)
    OPS = 200_000

    print()
    print("=" * 78)
    print(" LRU data structure — isolated benchmark")
    print("=" * 78)
    print()

    # ---- writes into a cache that never fills (no eviction) ----
    def writes_no_evict():
        c = LRUCache(OPS + 1)
        for i in range(OPS):
            c.put(i, i)

    print("Writes")
    bench("put, no eviction", writes_no_evict, OPS)

    # ---- writes into a full cache (every insert evicts) ----
    def writes_all_evict():
        c = LRUCache(1000)
        for i in range(OPS):
            c.put(i, i)

    bench("put, evicting every time", writes_all_evict, OPS)

    # ---- reads that all hit ----
    c_hit = LRUCache(10_000)
    for i in range(10_000):
        c_hit.put(i, i)
    hit_keys = [random.randrange(10_000) for _ in range(OPS)]

    def reads_hit():
        for k in hit_keys:
            c_hit.get(k)

    print()
    print("Reads")
    bench("get, all hits", reads_hit, OPS)

    # ---- reads that all miss ----
    def reads_miss():
        for k in hit_keys:
            c_hit.get(-k - 1)

    bench("get, all misses", reads_miss, OPS)

    # ---- realistic mix ----
    c_mix = LRUCache(10_000)
    for i in range(10_000):
        c_mix.put(i, i)
    mix_keys = [random.randrange(20_000) for _ in range(OPS)]

    def mixed():
        for i, k in enumerate(mix_keys):
            if i % 4 == 0:
                c_mix.put(k, k)
            else:
                c_mix.get(k)

    print()
    print("Mixed workload")
    bench("75% get / 25% put, 50% hit rate", mixed, OPS)

    # ---- does cost grow with size? the O(1) claim, measured ----
    print()
    print("Scaling — throughput should stay flat if operations are O(1)")
    for capacity in (1_000, 10_000, 100_000, 1_000_000):
        c = LRUCache(capacity)
        for i in range(capacity):
            c.put(i, i)
        keys = [random.randrange(capacity) for _ in range(50_000)]

        start = time.perf_counter()
        for k in keys:
            c.get(k)
        elapsed = time.perf_counter() - start

        print(f"  {capacity:>9,} entries {' ':>18} {50_000 / elapsed:>12,.0f} ops/sec")

    print()
    print("If the numbers above are roughly flat, cost is independent of size.")
    print("A naive LRU scanning a list for the oldest entry would fall off a")
    print("cliff between 1,000 and 1,000,000.")
    print()


if __name__ == "__main__":
    main()
