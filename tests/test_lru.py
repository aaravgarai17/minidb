"""Tests for the O(1) LRU cache.

Grouped by what they protect:
  - basic behaviour
  - eviction *ordering* (the part that's easy to get subtly wrong)
  - structural invariants (dict and list staying in agreement)
  - the O(1) claim itself
"""

import random
import time

import pytest

from minidb.lru import LRUCache


# --------------------------------------------------------------- basics


def test_put_and_get():
    c = LRUCache(2)
    c.put("a", 1)
    assert c.get("a") == 1


def test_get_missing_returns_default():
    c = LRUCache(2)
    assert c.get("nope") is None
    assert c.get("nope", "fallback") == "fallback"


def test_update_existing_key_does_not_grow():
    c = LRUCache(2)
    c.put("a", 1)
    c.put("a", 2)
    assert len(c) == 1
    assert c.get("a") == 2


def test_delete():
    c = LRUCache(2)
    c.put("a", 1)
    assert c.delete("a") is True
    assert c.delete("a") is False
    assert c.get("a") is None
    assert len(c) == 0


def test_contains_does_not_affect_recency():
    """EXISTS must not rescue a key from eviction."""
    c = LRUCache(2)
    c.put("a", 1)
    c.put("b", 2)

    assert "a" in c          # if this touched 'a', 'b' would be evicted next
    c.put("c", 3)

    assert "a" not in c      # 'a' was still the LRU, so it went
    assert "b" in c
    assert "c" in c


def test_peek_does_not_affect_recency():
    c = LRUCache(2)
    c.put("a", 1)
    c.put("b", 2)

    assert c.peek("a") == 1
    c.put("c", 3)

    assert c.peek("a") is None   # 'a' evicted despite being peeked
    assert c.peek("b") == 2


def test_capacity_must_be_positive():
    with pytest.raises(ValueError):
        LRUCache(0)
    with pytest.raises(ValueError):
        LRUCache(-1)


def test_clear():
    c = LRUCache(3)
    for k in "abc":
        c.put(k, 1)
    c.clear()
    assert len(c) == 0
    assert c.lru_key is None
    c._assert_consistent()


# ------------------------------------------------------ eviction ordering


def test_evicts_least_recently_used():
    c = LRUCache(2)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)          # capacity exceeded → 'a' is oldest, evict it

    assert c.get("a") is None
    assert c.get("b") == 2
    assert c.get("c") == 3


def test_get_refreshes_recency():
    c = LRUCache(2)
    c.put("a", 1)
    c.put("b", 2)

    c.get("a")             # 'a' is now most recent, 'b' is the LRU
    c.put("c", 3)

    assert c.get("a") == 1     # survived
    assert c.get("b") is None  # evicted instead
    assert c.get("c") == 3


def test_update_refreshes_recency():
    c = LRUCache(2)
    c.put("a", 1)
    c.put("b", 2)

    c.put("a", 99)         # updating counts as use
    c.put("c", 3)

    assert c.get("a") == 99
    assert c.get("b") is None


def test_put_returns_evicted_key():
    c = LRUCache(2)
    assert c.put("a", 1) is None
    assert c.put("b", 2) is None
    assert c.put("c", 3) == "a"     # caller learns what was dropped


def test_lru_and_mru_pointers():
    c = LRUCache(3)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)

    assert c.mru_key == "c"
    assert c.lru_key == "a"

    c.get("a")
    assert c.mru_key == "a"
    assert c.lru_key == "b"


def test_keys_are_ordered_most_to_least_recent():
    c = LRUCache(3)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)
    assert list(c.keys()) == ["c", "b", "a"]

    c.get("a")
    assert list(c.keys()) == ["a", "c", "b"]


def test_eviction_cascade_keeps_only_newest():
    c = LRUCache(3)
    for i in range(10):
        c.put(i, i)

    assert len(c) == 3
    assert set(c.keys()) == {7, 8, 9}


def test_capacity_one():
    """Degenerate case where every insert evicts."""
    c = LRUCache(1)
    c.put("a", 1)
    c.put("b", 2)

    assert c.get("a") is None
    assert c.get("b") == 2
    assert len(c) == 1
    c._assert_consistent()


# --------------------------------------------------- structural invariants


def test_structure_consistent_after_mixed_operations():
    """Fuzz the cache and assert the dict and list never disagree.

    This is the test that catches the classic bug in this structure: unlinking
    a node from the list but forgetting to remove it from the map (or vice
    versa). Such a bug is invisible until much later, when a stale node
    corrupts the ordering or leaks memory.
    """
    random.seed(1234)
    c = LRUCache(16)

    for _ in range(4000):
        op = random.random()
        key = random.randint(0, 40)

        if op < 0.45:
            c.put(key, key * 2)
        elif op < 0.80:
            c.get(key)
        elif op < 0.92:
            c.delete(key)
        else:
            key in c

        assert len(c) <= c.capacity

    c._assert_consistent()


def test_delete_then_reinsert_keeps_structure_sound():
    c = LRUCache(3)
    for k in "abc":
        c.put(k, 1)

    c.delete("b")
    c._assert_consistent()

    c.put("d", 4)
    c._assert_consistent()
    assert set(c.keys()) == {"a", "c", "d"}


def test_delete_head_and_tail():
    c = LRUCache(3)
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)

    c.delete("c")          # head (most recent)
    c._assert_consistent()
    c.delete("a")          # tail (least recent)
    c._assert_consistent()

    assert list(c.keys()) == ["b"]


def test_never_exceeds_capacity():
    c = LRUCache(5)
    for i in range(1000):
        c.put(i, i)
        assert len(c) <= 5


# ------------------------------------------------------------- statistics


def test_stats_track_hits_misses_evictions():
    c = LRUCache(2)
    c.put("a", 1)
    c.put("b", 2)

    c.get("a")             # hit
    c.get("zzz")           # miss
    c.put("c", 3)          # evicts 'b'

    s = c.stats()
    assert s["hits"] == 1
    assert s["misses"] == 1
    assert s["evictions"] == 1
    assert s["size"] == 2
    assert s["capacity"] == 2
    assert s["hit_rate_pct"] == 50


# ---------------------------------------------------------- the O(1) claim


def test_operations_are_constant_time():
    """Empirical check that cost doesn't grow with cache size.

    A naive LRU that scans a list to find the oldest entry is O(n), and the
    difference is invisible on small inputs. Here we time a fixed number of
    operations against a small cache and a cache 100x larger; if the
    implementation were O(n), the large one would be dramatically slower.

    The threshold is deliberately loose (5x) because wall-clock timing in CI is
    noisy — we're catching an O(n) regression, not measuring performance.
    """
    def time_ops(capacity: int, ops: int = 20_000) -> float:
        c = LRUCache(capacity)
        for i in range(capacity):        # fill it first
            c.put(i, i)

        keys = [random.randint(0, capacity - 1) for _ in range(ops)]
        start = time.perf_counter()
        for k in keys:
            c.get(k)
            c.put(k, k)
        return time.perf_counter() - start

    small = time_ops(100)
    large = time_ops(10_000)

    assert large < small * 5, (
        f"operations appear to scale with size — likely not O(1): "
        f"100 entries took {small:.4f}s, 10,000 took {large:.4f}s"
    )
