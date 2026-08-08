"""The TCP server.

Concurrency model — and why there are no locks
-----------------------------------------------
This runs a single asyncio event loop. Each client connection gets its own
coroutine, but only one coroutine executes at a time, and control transfers
between them only at an explicit `await`. Command handlers contain no `await`,
so a command runs start to finish without interruption.

That makes every command **atomic by construction**. No mutex, no race
conditions, no lock contention, and none of the subtle bugs that come with
shared mutable state across threads.

This is not a Python workaround — it is the design real Redis uses, for the
same reason. The insight is that an in-memory store spends nearly all its time
in I/O, not computation. Handling a `GET` is a hash lookup measured in
nanoseconds; reading the request off a socket takes microseconds. Threads would
add locking overhead to protect a data structure that was never the bottleneck.

The honest trade-off: a single loop uses one CPU core. Real Redis accepts this
too, and the standard answer to needing more throughput is to run more
instances and shard the keyspace across them, rather than to add threads.

What *would* break this
-----------------------
Any `await` inside a command handler would open a window for another coroutine
to observe half-finished state, and atomicity would be gone. That is why the
AOF writes are buffered synchronous file writes rather than async I/O — the
cure would be worse than the disease.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Optional

from minidb import commands, protocol
from minidb.aof import AOF
from minidb.config import Config
from minidb.store import Store

log = logging.getLogger("minidb")


class MiniDBServer:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.store = Store(capacity=config.max_keys)
        self.aof = AOF(
            config.aof_path,
            fsync_policy=config.fsync_policy,
            enabled=config.aof_enabled,
        )

        self._server: Optional[asyncio.AbstractServer] = None
        self._background: list[asyncio.Task] = []
        self._connections: set[asyncio.Task] = set()
        self._shutting_down = False

        self.commands_processed = 0
        self.connections_total = 0
        self._aof_size_at_start = 0

    # ------------------------------------------------------------- lifecycle

    def load(self) -> int:
        """Rebuild state from the AOF. Returns the number of commands replayed.

        Replay dispatches through the *same* command path used at runtime, so
        there is no separate deserializer that could drift out of sync with the
        live implementation.
        """
        if not self.aof.enabled:
            return 0

        replayed = 0
        for args in self.aof.replay():
            commands.dispatch(self.store, args)
            replayed += 1

        if replayed:
            log.info("replayed %d commands from AOF (%d keys restored)",
                     replayed, len(self.store))
        self._aof_size_at_start = self.aof.size_bytes()
        return replayed

    async def start(self) -> None:
        self.load()
        self.aof.open()

        self._server = await asyncio.start_server(
            self._handle_client, self.config.host, self.config.port
        )
        self._background = [
            asyncio.create_task(self._expiry_loop()),
            asyncio.create_task(self._fsync_loop()),
        ]

        addr = ", ".join(str(s.getsockname()) for s in self._server.sockets)
        log.info("minidb listening on %s (capacity=%d, fsync=%s)",
                 addr, self.config.max_keys, self.config.fsync_policy)

    async def serve_forever(self) -> None:
        async with self._server:
            await self._server.serve_forever()

    async def shutdown(self) -> None:
        """Stop accepting, let in-flight commands finish, flush, exit.

        Ordering matters and is the whole point of a *graceful* shutdown:
        close the listener first so no new work arrives, then drain, then sync
        to disk. Syncing before draining would leave the last commands
        unpersisted.
        """
        if self._shutting_down:
            return
        self._shutting_down = True
        log.info("shutting down...")

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

        for task in self._background:
            task.cancel()

        if self._connections:
            log.info("draining %d connection(s)", len(self._connections))
            await asyncio.wait(self._connections, timeout=5.0)
            for task in self._connections:
                task.cancel()

        # Final fsync: makes "no acknowledged write is lost on clean shutdown"
        # true even under the everysec policy.
        self.aof.close()
        log.info("shutdown complete (%d commands processed)", self.commands_processed)

    # ----------------------------------------------------------- connections

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        self._connections.add(task)
        self.connections_total += 1
        peer = writer.get_extra_info("peername")

        try:
            while not self._shutting_down:
                try:
                    args = await protocol.read_command(reader)
                except protocol.ProtocolError as exc:
                    writer.write(protocol.encode_error(f"ERR Protocol error: {exc}"))
                    await writer.drain()
                    break                       # unparseable stream: give up
                except (ConnectionResetError, asyncio.IncompleteReadError):
                    break

                if args is None:
                    break                       # client disconnected
                if not args:
                    continue                    # blank line

                reply, should_persist = commands.dispatch(self.store, args)
                if should_persist:
                    self.aof.log(args)

                self.commands_processed += 1

                writer.write(reply)
                # drain() applies backpressure: if the client isn't reading,
                # this suspends rather than letting the write buffer grow
                # without bound. A slow consumer throttles itself instead of
                # exhausting the server's memory.
                await writer.drain()

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("error serving %s", peer)
        finally:
            self._connections.discard(task)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    # ------------------------------------------------------- background work

    async def _expiry_loop(self) -> None:
        """Active expiry: reclaim TTL'd keys nobody reads.

        Runs frequently but does very little unless keys are actually expiring
        — see Store.active_expire_cycle for the sampling logic.
        """
        try:
            while True:
                await asyncio.sleep(self.config.expiry_interval)
                self.store.active_expire_cycle()
        except asyncio.CancelledError:
            pass

    async def _fsync_loop(self) -> None:
        """Honour the everysec policy even when writes stop arriving.

        Without this, a burst of writes followed by silence would leave the
        last few unsynced indefinitely, because the write path only checks the
        timer when a write happens.
        """
        try:
            while True:
                await asyncio.sleep(1.0)
                self.aof.maybe_sync()
                self._maybe_rewrite_aof()
        except asyncio.CancelledError:
            pass

    def _maybe_rewrite_aof(self) -> None:
        """Compact the log once it has grown well past its useful size."""
        if not self.aof.enabled:
            return

        size = self.aof.size_bytes()
        if size < self.config.auto_rewrite_min_size:
            return

        baseline = max(self._aof_size_at_start, self.config.auto_rewrite_min_size)
        if size < baseline * self.config.auto_rewrite_multiplier:
            return

        log.info("rewriting AOF (%d bytes)", size)
        self.aof.rewrite(list(self._snapshot_commands()))
        self._aof_size_at_start = self.aof.size_bytes()

    def _snapshot_commands(self):
        """Minimal command sequence that recreates current state.

        One SET per live key, plus an EXPIRE where a TTL exists. This is what
        turns an unbounded history into a bounded snapshot.
        """
        for key in self.store.keys("*"):
            value = self.store.get(key)
            if value is None:
                continue
            yield [b"SET", key, value]

            ttl = self.store.ttl(key)
            if ttl > 0:
                yield [b"EXPIRE", key, str(ttl).encode()]


# ----------------------------------------------------------------- entrypoint


async def run(config: Config) -> None:
    server = MiniDBServer(config)
    await server.start()

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    # SIGTERM is what `docker stop` and orchestrators send; SIGINT is Ctrl-C.
    # Handling both means the shutdown path is exercised in every environment.
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:      # e.g. Windows
            pass

    serving = asyncio.create_task(server.serve_forever())
    await stop.wait()

    serving.cancel()
    await server.shutdown()


def main(argv=None) -> None:
    from minidb.config import parse_args

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    config = parse_args(argv)

    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
