"""Tests for the store layer: TTL semantics, eviction interaction, expiry sweeps.

Time is injected rather than slept through. Tests that call `time.sleep` are
slow and flaky; a fake clock makes expiry deterministic and instant.
"""

import pytest

from minidb.store import Store


class FakeClock:
    """Controllable clock so expiry can be tested without sleeping."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def store(clock):
    return Store(capacity=100, clock=clock)


# ------------------------------------------------------------ basic behaviour


def test_set_and_get(store):
    store.set("a", "1")
    assert store.get("a") == "1"


def test_get_missing(store):
    assert store.get("nope") is None


def test_delete(store):
    store.set("a", "1")
    assert store.delete("a") is True
    assert store.delete("a") is False
    assert store.get("a") is None


def test_exists(store):
    store.set("a", "1")
    assert store.exists("a") is True
    assert store.exists("b") is False


def test_flush(store):
    store.set("a", "1")
    store.set("b", "2")
    assert store.flush() == 2
    assert len(store) == 0


# --------------------------------------------------------------- TTL semantics


def test_value_readable_before_expiry(store, clock):
    store.set("a", "1", ttl=10)
    clock.advance(9)
    assert store.get("a") == "1"


def test_value_gone_after_expiry(store, clock):
    store.set("a", "1", ttl=10)
    clock.advance(11)
    assert store.get("a") is None


def test_expiry_is_inclusive_at_the_boundary(store, clock):
    """At exactly the expiry instant the key is already gone."""
    store.set("a", "1", ttl=10)
    clock.advance(10)
    assert store.get("a") is None


def test_ttl_reports_remaining_seconds(store, clock):
    store.set("a", "1", ttl=100)
    clock.advance(40)
    assert store.ttl("a") == 60


def test_ttl_minus_one_when_no_expiry(store):
    store.set("a", "1")
    assert store.ttl("a") == -1


def test_ttl_minus_two_when_missing(store):
    assert store.ttl("nope") == -2


def test_ttl_minus_two_after_expiry(store, clock):
    store.set("a", "1", ttl=5)
    clock.advance(6)
    assert store.ttl("a") == -2


def test_set_without_ttl_clears_previous_ttl(store, clock):
    """Matches Redis: a plain SET replaces the value and drops the old TTL."""
    store.set("a", "1", ttl=10)
    store.set("a", "2")

    clock.advance(100)
    assert store.get("a") == "2"
    assert store.ttl("a") == -1


def test_expire_on_existing_key(store, clock):
    store.set("a", "1")
    assert store.expire("a", 10) is True

    clock.advance(11)
    assert store.get("a") is None


def test_expire_on_missing_key(store):
    assert store.expire("nope", 10) is False


def test_persist_removes_ttl(store, clock):
    store.set("a", "1", ttl=10)
    assert store.persist("a") is True

    clock.advance(100)
    assert store.get("a") == "1"


def test_persist_on_key_without_ttl(store):
    store.set("a", "1")
    assert store.persist("a") is False


def test_expired_key_not_counted_by_exists(store, clock):
    store.set("a", "1", ttl=5)
    clock.advance(6)
    assert store.exists("a") is False


def test_delete_returns_false_for_expired_key(store, clock):
    store.set("a", "1", ttl=5)
    clock.advance(6)
    assert store.delete("a") is False


# ------------------------------------------------------------- KEYS / patterns


def test_keys_returns_everything_by_default(store):
    store.set("a", 1)
    store.set("b", 2)
    assert set(store.keys()) == {"a", "b"}


def test_keys_glob_matching(store):
    for k in ["user:1", "user:2", "session:1"]:
        store.set(k, 1)

    assert set(store.keys("user:*")) == {"user:1", "user:2"}
    assert store.keys("session:*") == ["session:1"]
    assert store.keys("nomatch*") == []


def test_keys_excludes_expired(store, clock):
    store.set("live", 1)
    store.set("dead", 1, ttl=5)

    clock.advance(6)
    assert store.keys() == ["live"]


def test_keys_is_case_sensitive(store):
    store.set("ABC", 1)
    assert store.keys("abc") == []
    assert store.keys("ABC") == ["ABC"]


def test_keys_matches_bytes_keys(store):
    """Regression: keys arrive from the wire as bytes, not str.

    `str(b"user:1")` is `"b'user:1'"` — the repr — so matching without an
    explicit decode silently returned nothing for every real client. Passed in
    tests that used str keys; broken for all actual traffic.
    """
    store.set(b"user:1", b"a")
    store.set(b"user:2", b"b")
    store.set(b"other", b"c")

    assert set(store.keys("user:*")) == {b"user:1", b"user:2"}
    assert set(store.keys("*")) == {b"user:1", b"user:2", b"other"}
    assert store.keys("nomatch*") == []


def test_keys_returns_keys_in_stored_form(store):
    """Bytes in, bytes out — the caller encodes them for the wire."""
    store.set(b"k", b"v")
    assert store.keys("*") == [b"k"]


# ------------------------------------------------------- eviction interaction


def test_lru_eviction_when_over_capacity(clock):
    s = Store(capacity=3, clock=clock)
    for k in "abcd":
        s.set(k, 1)

    assert len(s) == 3
    assert s.get("a") is None       # oldest evicted


def test_eviction_cleans_up_expiry_table(clock):
    """A key evicted for memory reasons must not leak its expiry entry.

    Without this, the expiry table grows unboundedly in a workload that sets
    TTLs on keys which are later evicted — a slow memory leak that would only
    show up in production.
    """
    s = Store(capacity=2, clock=clock)
    s.set("a", 1, ttl=100)
    s.set("b", 2, ttl=100)
    s.set("c", 3, ttl=100)          # evicts 'a'

    assert s.stats()["keys_with_ttl"] == 2
    assert "a" not in s._expires


def test_reading_a_key_protects_it_from_eviction(clock):
    s = Store(capacity=2, clock=clock)
    s.set("a", 1)
    s.set("b", 2)

    s.get("a")                      # 'a' now most recent
    s.set("c", 3)                   # so 'b' is evicted

    assert s.get("a") == 1
    assert s.get("b") is None


# --------------------------------------------------------- active expiry sweep


def test_active_expiry_reclaims_unread_keys(clock):
    """The case lazy expiry alone cannot handle.

    These keys are written with a TTL and never read again. Without an active
    sweep they would occupy memory indefinitely, because lazy expiry only ever
    triggers on access.
    """
    s = Store(capacity=1000, clock=clock)
    for i in range(100):
        s.set(f"k{i}", i, ttl=10)

    assert s.raw_size() == 100

    clock.advance(11)
    assert s.raw_size() == 100      # nothing read them, so nothing noticed yet

    total = 0
    for _ in range(20):
        total += s.active_expire_cycle()

    assert total == 100
    assert s.raw_size() == 0


def test_active_expiry_leaves_live_keys_alone(clock):
    s = Store(capacity=1000, clock=clock)
    for i in range(50):
        s.set(f"live{i}", i, ttl=1000)
    for i in range(50):
        s.set(f"dead{i}", i, ttl=10)

    clock.advance(11)
    for _ in range(20):
        s.active_expire_cycle()

    remaining = set(s.keys())
    assert len(remaining) == 50
    assert all(k.startswith("live") for k in remaining)


def test_active_expiry_is_a_noop_with_no_ttls(clock):
    s = Store(capacity=100, clock=clock)
    for i in range(10):
        s.set(f"k{i}", i)           # no TTL

    assert s.active_expire_cycle() == 0
    assert s.raw_size() == 10


def test_active_expiry_stops_early_when_few_keys_expired(clock):
    """Sampling should give up quickly when expired keys are rare.

    One expired key out of 200 is far below the 25% threshold, so the cycle
    should do a round or two and stop rather than hunting exhaustively.
    """
    s = Store(capacity=1000, clock=clock)
    for i in range(200):
        s.set(f"k{i}", i, ttl=1000)
    s.set("doomed", 1, ttl=5)

    clock.advance(6)
    removed = s.active_expire_cycle()

    assert removed <= 1             # found it or didn't; either is acceptable
    assert s.raw_size() >= 200      # live keys untouched


def test_expired_count_tracked(store, clock):
    store.set("a", 1, ttl=5)
    clock.advance(6)
    store.get("a")                  # lazy expiry fires

    assert store.stats()["expired_total"] == 1
