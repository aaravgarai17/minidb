"""Append-only file persistence.

The idea
--------
Rather than periodically dumping the whole dataset, log every *write command*
as it happens. Recovery replays the log from the beginning, re-executing each
command to rebuild the exact state that existed at shutdown.

AOF vs snapshotting
-------------------
A snapshot (Redis calls it RDB) forks and writes the entire dataset to disk
every N minutes. It produces a compact file and restarts quickly, but any
writes since the last snapshot are lost on a crash.

An AOF is larger and slower to load, but its durability window is a single
command rather than several minutes. Since the whole point of adding
persistence here is "don't lose acknowledged writes", AOF is the right choice.
Production Redis usually runs both.

The durability/performance trade-off
------------------------------------
`write()` copies bytes into the OS page cache; it does **not** put them on the
platter. Only `fsync()` does that. So the fsync policy decides what a crash
costs, and there is no free option:

    always     fsync after every write command. Nothing acknowledged is ever
               lost. Also the slowest by a wide margin — a durable disk write
               is orders of magnitude slower than a memory write, so this caps
               throughput at the disk's sync rate.

    everysec   fsync once a second in the background. Up to one second of
               writes lost on a crash. This is Redis's default and the usual
               right answer: near-zero cost, bounded and comprehensible risk.

    no         never fsync explicitly; let the OS flush whenever it likes.
               Fastest, and the loss window is however long the kernel decides
               to buffer — typically up to 30 seconds on Linux.

Note that "the OS crashed" and "the process crashed" are different failures.
If only the *process* dies, data already handed to `write()` survives, because
the page cache belongs to the kernel. `fsync` protects against the machine
losing power, not against a Python exception.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Iterator, Optional, Sequence

from minidb.protocol import encode_array

# Commands that mutate state and therefore belong in the log. Reads are
# excluded: replaying a GET would accomplish nothing and would bloat the file.
WRITE_COMMANDS = {
    b"SET", b"DEL", b"EXPIRE", b"PERSIST", b"FLUSHALL", b"SETEX", b"MSET",
    b"INCR", b"DECR", b"INCRBY", b"DECRBY",
}

FSYNC_ALWAYS = "always"
FSYNC_EVERYSEC = "everysec"
FSYNC_NO = "no"


class AOF:
    """Append-only log of write commands.

    Commands are stored in RESP array format — the same bytes a client would
    have sent. That means the replay path is the *same* code path as the live
    command path, so a bug can't hide in a separate deserializer, and the file
    is inspectable with `cat`.
    """

    def __init__(
        self,
        path: Optional[str | Path],
        fsync_policy: str = FSYNC_EVERYSEC,
        enabled: bool = True,
    ) -> None:
        self.path = Path(path) if path else None
        self.fsync_policy = fsync_policy
        self.enabled = enabled and self.path is not None

        self._fh = None
        self._last_fsync = time.monotonic()
        self.writes_logged = 0
        self.fsyncs = 0

    # ------------------------------------------------------------- lifecycle

    def open(self) -> None:
        if not self.enabled:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "ab")

    def close(self) -> None:
        """Flush, sync, and close. Called on graceful shutdown.

        The final fsync is what makes "graceful shutdown loses no acknowledged
        writes" true even under the `everysec` policy.
        """
        if self._fh is None:
            return
        try:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self.fsyncs += 1
        finally:
            self._fh.close()
            self._fh = None

    # ----------------------------------------------------------------- write

    def log(self, args: Sequence[bytes]) -> None:
        """Append one command, if it is a write."""
        if not self.enabled or self._fh is None or not args:
            return
        if args[0].upper() not in WRITE_COMMANDS:
            return

        self._fh.write(encode_array(list(args)))
        self.writes_logged += 1

        if self.fsync_policy == FSYNC_ALWAYS:
            self._sync()
        elif self.fsync_policy == FSYNC_EVERYSEC:
            # Checked on write rather than by a timer: a server taking no
            # writes has nothing to sync, so a background tick would be pure
            # overhead.
            if time.monotonic() - self._last_fsync >= 1.0:
                self._sync()

    def _sync(self) -> None:
        if self._fh is None:
            return
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._last_fsync = time.monotonic()
        self.fsyncs += 1

    def maybe_sync(self) -> None:
        """Hook for the server's periodic task under the everysec policy."""
        if (
            self.enabled
            and self.fsync_policy == FSYNC_EVERYSEC
            and time.monotonic() - self._last_fsync >= 1.0
        ):
            self._sync()

    # ---------------------------------------------------------------- replay

    def replay(self) -> Iterator[list[bytes]]:
        """Yield each logged command, in order, for re-execution on startup.

        A truncated final command — the normal result of a crash mid-write — is
        skipped rather than treated as corruption. Every complete command
        before it is still perfectly good, and discarding the whole log because
        its last few bytes are missing would turn a minor event into total data
        loss.
        """
        if not self.path or not self.path.exists():
            return

        with open(self.path, "rb") as fh:
            data = fh.read()

        pos = 0
        end = len(data)

        while pos < end:
            try:
                args, pos = _parse_one(data, pos)
            except _Truncated:
                break                     # partial tail, stop cleanly
            if args:
                yield args

    # ------------------------------------------------------------ compaction

    def rewrite(self, commands: Sequence[Sequence[bytes]]) -> None:
        """Replace the log with a minimal set of commands reproducing state.

        Necessary because the log records *history*, not state. A key written a
        thousand times contributes a thousand entries, of which only the last
        matters. Left alone the file grows without bound and startup gets
        slower forever.

        Written to a temp file and atomically renamed, so a crash mid-rewrite
        leaves the original log intact rather than a half-written one.
        """
        if not self.path:
            return

        tmp = self.path.with_suffix(self.path.suffix + ".rewrite")
        with open(tmp, "wb") as fh:
            for args in commands:
                fh.write(encode_array(list(args)))
            fh.flush()
            os.fsync(fh.fileno())

        was_open = self._fh is not None
        if was_open:
            self._fh.close()
            self._fh = None

        os.replace(tmp, self.path)        # atomic on POSIX

        if was_open:
            self.open()

    def size_bytes(self) -> int:
        if not self.path or not self.path.exists():
            return 0
        return self.path.stat().st_size

    def stats(self) -> dict:
        return {
            "enabled": self.enabled,
            "path": str(self.path) if self.path else None,
            "fsync_policy": self.fsync_policy,
            "writes_logged": self.writes_logged,
            "fsyncs": self.fsyncs,
            "size_bytes": self.size_bytes(),
        }


class _Truncated(Exception):
    """The buffer ends mid-command."""


def _parse_one(data: bytes, pos: int) -> tuple[list[bytes], int]:
    """Parse one RESP array from `data` starting at `pos`.

    A synchronous counterpart to the stream parser in protocol.py — replay
    reads from a byte buffer, not a socket.
    """
    if data[pos : pos + 1] != b"*":
        raise _Truncated()

    nl = data.find(b"\r\n", pos)
    if nl == -1:
        raise _Truncated()

    try:
        count = int(data[pos + 1 : nl])
    except ValueError:
        raise _Truncated()

    pos = nl + 2
    args: list[bytes] = []

    for _ in range(count):
        if data[pos : pos + 1] != b"$":
            raise _Truncated()

        nl = data.find(b"\r\n", pos)
        if nl == -1:
            raise _Truncated()

        try:
            length = int(data[pos + 1 : nl])
        except ValueError:
            raise _Truncated()

        start = nl + 2
        stop = start + length
        if stop + 2 > len(data):
            raise _Truncated()

        args.append(data[start:stop])
        pos = stop + 2

    return args, pos
