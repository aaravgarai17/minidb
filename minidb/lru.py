"""O(1) LRU cache: hash map + intrusive doubly linked list.

The problem
-----------
An LRU cache must answer two questions quickly:

  1. "What is the value for key K?"           → needs fast *lookup*
  2. "Which key was used least recently?"     → needs fast *ordering*

No single structure gives both. A dict has O(1) lookup but no order. A list has
order but O(n) lookup. The classic solution runs them side by side:

  - a **dict** mapping key → node, for O(1) lookup
  - a **doubly linked list** of those same nodes, ordered by recency

The dict's values *are* the list's nodes, so once you've found a node by key
you already hold a pointer into the middle of the list — no scan required.

Why the list must be doubly linked
----------------------------------
To move a node to the front (on access) or drop it from the tail (on eviction),
you must splice it out of its current position. Splicing requires rewiring the
neighbours on *both* sides, so a node needs a pointer to its predecessor as
well as its successor. With a singly linked list you'd have to walk from the
head to find the predecessor — O(n), which is exactly what we're avoiding.

Why sentinel nodes
------------------
The list keeps permanent dummy `head` and `tail` nodes that never hold data.
Without them, every insert and remove needs branches for "is this the first
node?", "is this the last node?", "is the list empty?". With them, every real
node is guaranteed to have both a previous and a next node, so splicing is
always the same four pointer assignments with no special cases. It costs two
objects and removes an entire class of off-by-one bugs.

A note on OrderedDict
---------------------
Python's `collections.OrderedDict` (and, since 3.7, plain `dict`) already
maintains insertion order and offers `move_to_end`, so a production Python
cache would reasonably use it. This module implements the structure by hand
deliberately: the point of the project is understanding the mechanism, not
delegating to one. The hand-rolled version is also what a systems language
would require, and it's what an interviewer asks you to whiteboard.
"""

from __future__ import annotations

from typing import Any, Iterator, Optional


class Node:
    """One entry in the cache, and simultaneously one link in the list.

    `__slots__` avoids a per-instance `__dict__`. With one node per cached key
    that saves roughly 50 bytes each — worth having in a structure whose whole
    purpose is holding many entries in memory.
    """

    __slots__ = ("key", "value", "prev", "next")

    def __init__(self, key: Any = None, value: Any = None) -> None:
        self.key = key
        self.value = value
        self.prev: Optional[Node] = None
        self.next: Optional[Node] = None

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Node({self.key!r}={self.value!r})"


class LRUCache:
    """Fixed-capacity cache evicting the least recently used entry.

    All of `get`, `put`, `delete`, and `__contains__` are O(1).

    Recency order is maintained head-to-tail: the node just behind `head` is
    the most recently used, and the node just before `tail` is the eviction
    candidate.
    """

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")

        self.capacity = capacity
        self._map: dict[Any, Node] = {}

        # Sentinels. These two never hold data and are never evicted; they only
        # exist so that real nodes always have neighbours on both sides.
        self._head = Node()
        self._tail = Node()
        self._head.next = self._tail
        self._tail.prev = self._head

        self.hits = 0
        self.misses = 0
        self.evictions = 0

    # ---------------------------------------------------------------- list ops

    def _unlink(self, node: Node) -> None:
        """Splice a node out of the list. O(1), no special cases."""
        node.prev.next = node.next
        node.next.prev = node.prev
        node.prev = node.next = None

    def _push_front(self, node: Node) -> None:
        """Insert a node directly after head, marking it most recently used."""
        first = self._head.next
        node.prev = self._head
        node.next = first
        self._head.next = node
        first.prev = node

    def _touch(self, node: Node) -> None:
        """Mark an existing node as most recently used."""
        self._unlink(node)
        self._push_front(node)

    # ------------------------------------------------------------- public API

    def get(self, key: Any, default: Any = None) -> Any:
        """Return the value for `key`, marking it most recently used."""
        node = self._map.get(key)
        if node is None:
            self.misses += 1
            return default

        self.hits += 1
        self._touch(node)
        return node.value

    def put(self, key: Any, value: Any) -> Optional[Any]:
        """Insert or update `key`.

        Returns the evicted key if this insertion forced an eviction, else None
        — the caller (the store layer) needs to know so it can drop any
        associated metadata such as expiry timestamps.
        """
        node = self._map.get(key)

        if node is not None:
            # Update in place. No size change, so no eviction possible.
            node.value = value
            self._touch(node)
            return None

        evicted_key = None
        if len(self._map) >= self.capacity:
            evicted_key = self._evict_lru()

        node = Node(key, value)
        self._map[key] = node
        self._push_front(node)
        return evicted_key

    def delete(self, key: Any) -> bool:
        """Remove `key`. Returns True if it was present."""
        node = self._map.pop(key, None)
        if node is None:
            return False
        self._unlink(node)
        return True

    def _evict_lru(self) -> Optional[Any]:
        """Drop the least recently used entry — the node before the tail."""
        lru = self._tail.prev
        if lru is self._head:  # empty list
            return None

        self._unlink(lru)
        del self._map[lru.key]
        self.evictions += 1
        return lru.key

    def clear(self) -> None:
        self._map.clear()
        self._head.next = self._tail
        self._tail.prev = self._head

    def keys(self) -> Iterator[Any]:
        """Iterate keys from most to least recently used.

        Walks the list rather than the dict, so the order is meaningful. This
        is O(n) and is intended for introspection (KEYS, expiry sweeps), not
        for the hot path.
        """
        node = self._head.next
        while node is not self._tail:
            yield node.key
            node = node.next

    def peek(self, key: Any, default: Any = None) -> Any:
        """Read a value *without* affecting recency order.

        Needed by the expiry sweeper: a background process checking whether a
        key has expired must not make that key look recently used, or sweeping
        would keep garbage alive and evict live data instead.
        """
        node = self._map.get(key)
        return default if node is None else node.value

    @property
    def lru_key(self) -> Optional[Any]:
        """The next key that would be evicted. Exposed for tests."""
        lru = self._tail.prev
        return None if lru is self._head else lru.key

    @property
    def mru_key(self) -> Optional[Any]:
        """The most recently used key. Exposed for tests."""
        mru = self._head.next
        return None if mru is self._tail else mru.key

    def stats(self) -> dict[str, int]:
        total = self.hits + self.misses
        return {
            "size": len(self._map),
            "capacity": self.capacity,
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "hit_rate_pct": round(100 * self.hits / total) if total else 0,
        }

    # ------------------------------------------------------------- dunder API

    def __len__(self) -> int:
        return len(self._map)

    def __contains__(self, key: Any) -> bool:
        """Membership test that does NOT count as a use.

        Deliberately does not call `_touch`: `EXISTS` shouldn't rescue a key
        from eviction. Also doesn't record a hit or miss, since it isn't a read.
        """
        return key in self._map

    def _assert_consistent(self) -> None:
        """Verify the dict and the list agree. Test-only invariant check.

        Catches the classic bug in this structure: removing a node from one
        half and forgetting the other, leaving a leak or a dangling pointer
        that only surfaces much later.
        """
        forward = []
        node = self._head.next
        seen = set()
        while node is not self._tail:
            if id(node) in seen:
                raise AssertionError("cycle detected in LRU list")
            seen.add(id(node))
            forward.append(node.key)
            node = node.next

        backward = []
        node = self._tail.prev
        while node is not self._head:
            backward.append(node.key)
            node = node.prev

        if forward != list(reversed(backward)):
            raise AssertionError(
                f"list is not consistent in both directions: "
                f"forward={forward}, backward={list(reversed(backward))}"
            )
        if len(forward) != len(self._map):
            raise AssertionError(
                f"list has {len(forward)} nodes but map has {len(self._map)}"
            )
        if set(forward) != set(self._map):
            raise AssertionError("list keys and map keys differ")
