"""The store: an LRU cache plus key expiry.

Two independent reasons a key can disappear
-------------------------------------------
  * **Eviction** — the cache is full and this was the least recently used key.
    Driven by memory pressure. The key is gone even though the user never said
    it should be.
  * **Expiry** — the key had a TTL and its time is up. Driven by the clock.
    Would have happened regardless of memory pressure.

They're handled by different mechanisms (the LRU list vs. an expiry table) but
must stay in sync: evicting a key has to drop its expiry entry too, or the
table leaks entries for keys that no longer exist.

Why expiry is checked in two different ways
-------------------------------------------
**Lazy expiry** happens on read: before returning a value, check whether it has
expired and delete it if so. Cheap and exactly correct from the client's point
of view — an expired key is never visible. But a key that is written once with
a TTL and then never read again will sit in memory forever, because nothing
ever looks at it.

**Active expiry** fixes that with a background sweep. The naive version — scan
every key every second — is O(n) per tick and would stall a large database, so
this follows Redis's approach: sample a small batch of random keys, delete the
expired ones, and if a high proportion of the sample turned out to be expired,
immediately sample again. When lots of keys are expiring the loop works hard;
when few are, it does almost nothing. It's probabilistic rather than exact, so
memory is reclaimed promptly without ever pausing to walk the whole keyspace.

Neither approach alone is sufficient: lazy alone leaks, active alone can serve
a stale value in the window before the sweeper reaches it. Together, reads are
always correct *and* memory is bounded.
"""

from __future__ import annotations

import fnmatch
import random
import time
from typing import Any, Iterator, Optional

from minidb.lru import LRUCache

# Redis's defaults for the sampling sweep, and for the same reasons.
ACTIVE_EXPIRY_SAMPLE = 20
ACTIVE_EXPIRY_THRESHOLD = 0.25


class Store:
    """Key-value store with LRU eviction and TTL support.

    Not internally synchronised. The server runs a single asyncio event loop,
    so every command executes to completion before the next one starts and no
    locking is required — the same design decision real Redis makes. See the
    README for why that is a feature rather than a limitation.
    """

    def __init__(self, capacity: int = 100_000, clock=time.time) -> None:
        self._cache = LRUCache(capacity)
        self._expires: dict[Any, float] = {}
        self._clock = clock          # injectable so tests control time
        self.expired_count = 0

    # ------------------------------------------------------------- internals

    def _is_expired(self, key: Any) -> bool:
        expire_at = self._expires.get(key)
        return expire_at is not None and self._clock() >= expire_at

    def _drop(self, key: Any) -> None:
        """Remove a key from both the cache and the expiry table."""
        self._cache.delete(key)
        self._expires.pop(key, None)

    def _expire_if_due(self, key: Any) -> bool:
        """Lazy expiry. Returns True if the key was expired and removed."""
        if self._is_expired(key):
            self._drop(key)
            self.expired_count += 1
            return True
        return False

    # ------------------------------------------------------------ public API

    def set(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        """Store a value, optionally with a time-to-live in seconds."""
        evicted = self._cache.put(key, value)
        if evicted is not None:
            # Keep the expiry table from leaking entries for evicted keys.
            self._expires.pop(evicted, None)

        if ttl is None:
            # SET without EX clears any previous TTL — matches Redis, where a
            # plain SET replaces the value *and* its expiry.
            self._expires.pop(key, None)
        else:
            self._expires[key] = self._clock() + ttl

    def get(self, key: Any) -> Optional[Any]:
        if self._expire_if_due(key):
            return None
        return self._cache.get(key)

    def delete(self, key: Any) -> bool:
        """Remove a key. Returns True if it existed and had not expired."""
        if self._expire_if_due(key):
            return False
        existed = self._cache.delete(key)
        self._expires.pop(key, None)
        return existed

    def exists(self, key: Any) -> bool:
        if self._expire_if_due(key):
            return False
        return key in self._cache

    def ttl(self, key: Any) -> int:
        """Seconds until expiry.

        Follows Redis's convention:
          -2  key does not exist
          -1  key exists but has no expiry
           n  seconds remaining (rounded up)
        """
        if self._expire_if_due(key) or key not in self._cache:
            return -2

        expire_at = self._expires.get(key)
        if expire_at is None:
            return -1

        remaining = expire_at - self._clock()
        return max(0, int(remaining + 0.999))   # round up, never report 0 early

    def expire(self, key: Any, ttl: float) -> bool:
        """Attach a TTL to an existing key. Returns False if it doesn't exist."""
        if self._expire_if_due(key) or key not in self._cache:
            return False
        self._expires[key] = self._clock() + ttl
        return True

    def persist(self, key: Any) -> bool:
        """Remove a key's TTL, making it permanent. False if it had none."""
        if self._expire_if_due(key) or key not in self._cache:
            return False
        return self._expires.pop(key, None) is not None

    @staticmethod
    def _as_text(key: Any) -> str:
        """Render a key for pattern matching.

        Keys arrive off the wire as bytes, and `str(b"user:1")` yields
        `"b'user:1'"` — the repr, complete with prefix and quotes — which
        matches no sensible glob. Decoding explicitly is required for KEYS to
        work at all on real traffic.
        """
        if isinstance(key, bytes):
            return key.decode("utf-8", errors="replace")
        return str(key)

    def keys(self, pattern: str = "*") -> list[Any]:
        """Keys matching a glob pattern, excluding expired ones.

        Returns keys in their stored form (bytes for wire traffic) so callers
        can encode them directly; only the comparison is done on text.

        O(n) and it materialises the whole list, exactly like Redis's KEYS.
        Fine for debugging, a bad idea on a large live database — the README
        says so rather than pretending otherwise.
        """
        # Snapshot first: expiring keys mutates the structure we're iterating.
        candidates = list(self._cache.keys())
        out = []
        for key in candidates:
            if self._expire_if_due(key):
                continue
            if fnmatch.fnmatchcase(self._as_text(key), pattern):
                out.append(key)
        return out

    def flush(self) -> int:
        n = len(self._cache)
        self._cache.clear()
        self._expires.clear()
        return n

    def active_expire_cycle(self, sample_size: int = ACTIVE_EXPIRY_SAMPLE) -> int:
        """One round of probabilistic expiry sweeping.

        Samples random keys *that have a TTL* rather than random keys overall —
        keys without expiry can never be expired, so sampling them is wasted
        work. Repeats while the hit rate stays high, capped so a pathological
        keyspace can't monopolise the event loop.

        Returns how many keys were removed.
        """
        removed = 0

        for _ in range(16):   # hard cap on rounds per cycle
            if not self._expires:
                break

            keys_with_ttl = list(self._expires.keys())
            batch = (
                keys_with_ttl
                if len(keys_with_ttl) <= sample_size
                else random.sample(keys_with_ttl, sample_size)
            )

            found = 0
            for key in batch:
                if self._is_expired(key):
                    self._drop(key)
                    self.expired_count += 1
                    found += 1

            removed += found

            # Below the threshold, expired keys are sparse enough that further
            # sampling isn't worth the CPU. Stop and try again next tick.
            if not batch or (found / len(batch)) < ACTIVE_EXPIRY_THRESHOLD:
                break

        return removed

    # ------------------------------------------------------------ inspection

    def __len__(self) -> int:
        return len(self._cache)

    def __contains__(self, key: Any) -> bool:
        return self.exists(key)

    def raw_size(self) -> int:
        """Entry count including not-yet-reaped expired keys. Test/debug only."""
        return len(self._cache)

    def stats(self) -> dict[str, Any]:
        s = self._cache.stats()
        s["keys_with_ttl"] = len(self._expires)
        s["expired_total"] = self.expired_count
        return s
