"""End-to-end tests against a real server over a real TCP socket.

These are the tests that would catch anything the unit tests can't: protocol
framing across packet boundaries, concurrent clients, graceful shutdown, and
data actually surviving a restart.
"""

import asyncio

import pytest

from minidb.config import Config
from minidb.protocol import encode_array
from minidb.server import MiniDBServer


async def free_port() -> int:
    """Ask the OS for an unused port, avoiding collisions in parallel runs."""
    srv = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    srv.close()
    await srv.wait_closed()
    return port


class RawClient:
    """Minimal RESP client over asyncio streams."""

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    @classmethod
    async def connect(cls, port: int):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        return cls(reader, writer)

    async def send(self, *parts):
        args = [p.encode() if isinstance(p, str) else p for p in parts]
        self.writer.write(encode_array(args))
        await self.writer.drain()
        return await self.read_reply()

    async def send_raw(self, data: bytes):
        self.writer.write(data)
        await self.writer.drain()
        return await self.read_reply()

    async def read_reply(self):
        line = await self.reader.readline()
        if not line:
            raise ConnectionError("server closed connection")

        kind, rest = line[:1], line[1:].rstrip(b"\r\n")

        if kind in (b"+", b"-"):
            return (kind + rest).decode()
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n == -1:
                return None
            return (await self.reader.readexactly(n + 2))[:-2]
        if kind == b"*":
            n = int(rest)
            if n == -1:
                return None
            return [await self.read_reply() for _ in range(n)]
        raise AssertionError(f"unexpected reply byte {kind!r}")

    async def close(self):
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except Exception:
            pass


@pytest.fixture
async def server(tmp_path):
    port = await free_port()
    config = Config(
        host="127.0.0.1",
        port=port,
        max_keys=1000,
        aof_path=str(tmp_path / "test.aof"),
        expiry_interval=0.02,
    )
    srv = MiniDBServer(config)
    await srv.start()
    serving = asyncio.create_task(srv.serve_forever())

    yield srv

    serving.cancel()
    await srv.shutdown()


@pytest.fixture
async def client(server):
    c = await RawClient.connect(server.config.port)
    yield c
    await c.close()


# ------------------------------------------------------------------- basics


async def test_ping_over_the_wire(client):
    assert await client.send("PING") == "+PONG"


async def test_set_get_roundtrip(client):
    assert await client.send("SET", "k", "v") == "+OK"
    assert await client.send("GET", "k") == b"v"


async def test_missing_key_returns_nil(client):
    assert await client.send("GET", "absent") is None


async def test_del_and_exists(client):
    await client.send("SET", "k", "v")
    assert await client.send("EXISTS", "k") == 1
    assert await client.send("DEL", "k") == 1
    assert await client.send("EXISTS", "k") == 0


async def test_keys_over_the_wire(client):
    """Exercises the bytes-key glob path that unit tests with str keys missed."""
    await client.send("SET", "user:1", "a")
    await client.send("SET", "user:2", "b")
    await client.send("SET", "other", "c")

    result = await client.send("KEYS", "user:*")
    assert sorted(result) == [b"user:1", b"user:2"]


async def test_binary_values_survive_the_wire(client):
    blob = b"\x00\xff\r\nbinary\x00"
    await client.send("SET", b"bin", blob)
    assert await client.send("GET", b"bin") == blob


async def test_inline_command(client):
    """Plain text, the way netcat or telnet would send it."""
    assert await client.send_raw(b"PING\r\n") == "+PONG"


async def test_unknown_command_errors_without_dropping_connection(client):
    reply = await client.send("BOGUS")
    assert reply.startswith("-ERR unknown command")
    assert await client.send("PING") == "+PONG"        # still usable


# ------------------------------------------------------------------- expiry


async def test_ttl_expires_key_in_the_background(client):
    """Active expiry: nobody reads the key, yet it still disappears."""
    await client.send("SET", "temp", "v", "PX", "50")
    assert await client.send("GET", "temp") == b"v"

    await asyncio.sleep(0.25)
    assert await client.send("DBSIZE") == 0


async def test_ttl_countdown(client):
    await client.send("SET", "k", "v", "EX", "100")
    assert 0 < await client.send("TTL", "k") <= 100


# -------------------------------------------------------------- concurrency


async def test_many_concurrent_clients(server):
    """Each client hammers its own keyspace; none may observe another's data.

    The single event loop makes each command atomic, so this should hold with
    no locking anywhere in the codebase.
    """
    async def worker(n: int):
        c = await RawClient.connect(server.config.port)
        try:
            for i in range(20):
                key = f"c{n}:k{i}"
                assert await c.send("SET", key, f"v{n}-{i}") == "+OK"
            for i in range(20):
                got = await c.send("GET", f"c{n}:k{i}")
                assert got == f"v{n}-{i}".encode()
        finally:
            await c.close()

    await asyncio.gather(*(worker(n) for n in range(25)))
    assert await_dbsize(server) == 500


def await_dbsize(server) -> int:
    return len(server.store)


async def test_concurrent_incr_loses_no_updates(server):
    """The atomicity claim, proven on a single shared key.

    INCR is read-modify-write: fetch the value, parse it, add one, store it. In
    a threaded server this is exactly where a lost update happens — two threads
    read 5, both write 6, and one increment silently vanishes. Locking exists
    to prevent that.

    This server holds no locks at all. It doesn't need them: each command runs
    to completion inside one event-loop turn, because no handler contains an
    `await`. So 25 concurrent clients incrementing the same key 40 times each
    must produce exactly 1000. Any interleaving bug would show up here as a
    number that is too low, and would be reliably reproducible under load.
    """
    CLIENTS, PER_CLIENT = 25, 40

    async def bump():
        c = await RawClient.connect(server.config.port)
        try:
            for _ in range(PER_CLIENT):
                await c.send("INCR", "counter")
        finally:
            await c.close()

    await asyncio.gather(*(bump() for _ in range(CLIENTS)))

    final = await_counter(server)
    assert final == CLIENTS * PER_CLIENT, (
        f"lost updates: expected {CLIENTS * PER_CLIENT}, got {final}"
    )


def await_counter(server) -> int:
    return int(server.store.get(b"counter"))


async def test_incr_semantics(client):
    assert await client.send("INCR", "n") == 1        # missing key starts at 0
    assert await client.send("INCR", "n") == 2
    assert await client.send("INCRBY", "n", "10") == 12
    assert await client.send("DECR", "n") == 11
    assert await client.send("DECRBY", "n", "5") == 6
    assert await client.send("GET", "n") == b"6"


async def test_incr_on_non_numeric_value_errors(client):
    await client.send("SET", "word", "hello")
    reply = await client.send("INCR", "word")
    assert reply.startswith("-ERR value is not an integer")


async def test_incr_preserves_ttl(client):
    """Incrementing changes the value, not the key's lifetime."""
    await client.send("SET", "c", "1", "EX", "100")
    await client.send("INCR", "c")
    assert 0 < await client.send("TTL", "c") <= 100


async def test_mset_and_mget(client):
    assert await client.send("MSET", "a", "1", "b", "2") == "+OK"
    assert await client.send("MGET", "a", "b", "missing") == [b"1", b"2", None]


async def test_mset_rejects_odd_arguments(client):
    reply = await client.send("MSET", "a", "1", "b")
    assert reply.startswith("-ERR wrong number of arguments")


async def test_pipelined_commands(client):
    """Several commands in one write, replies read back in order."""
    payload = b"".join(
        encode_array([b"SET", f"p{i}".encode(), str(i).encode()])
        for i in range(10)
    )
    client.writer.write(payload)
    await client.writer.drain()

    for _ in range(10):
        assert await client.read_reply() == "+OK"

    assert await client.send("GET", "p7") == b"7"


# ------------------------------------------------------------- LRU eviction


async def test_eviction_at_capacity(tmp_path):
    port = await free_port()
    srv = MiniDBServer(
        Config(host="127.0.0.1", port=port, max_keys=10,
               aof_path=str(tmp_path / "e.aof"))
    )
    await srv.start()
    serving = asyncio.create_task(srv.serve_forever())

    try:
        c = await RawClient.connect(port)
        for i in range(50):
            await c.send("SET", f"k{i}", "v")

        assert await c.send("DBSIZE") == 10
        assert await c.send("GET", "k0") is None      # long since evicted
        assert await c.send("GET", "k49") == b"v"     # newest survives
        await c.close()
    finally:
        serving.cancel()
        await srv.shutdown()


# --------------------------------------------------------------- durability


async def test_data_survives_restart(tmp_path):
    """The core persistence guarantee: write, shut down, start again, read."""
    aof = str(tmp_path / "persist.aof")
    port = await free_port()

    srv = MiniDBServer(Config(host="127.0.0.1", port=port, aof_path=aof))
    await srv.start()
    serving = asyncio.create_task(srv.serve_forever())

    c = await RawClient.connect(port)
    await c.send("SET", "durable", "yes")
    await c.send("SET", "counter", "42")
    await c.send("DEL", "counter")
    await c.close()

    serving.cancel()
    await srv.shutdown()                    # flushes and fsyncs

    # --- restart on the same file ---
    port2 = await free_port()
    srv2 = MiniDBServer(Config(host="127.0.0.1", port=port2, aof_path=aof))
    replayed = srv2.load()
    assert replayed == 3

    await srv2.start()
    serving2 = asyncio.create_task(srv2.serve_forever())

    try:
        c2 = await RawClient.connect(port2)
        assert await c2.send("GET", "durable") == b"yes"
        assert await c2.send("GET", "counter") is None   # the DEL replayed too
        await c2.close()
    finally:
        serving2.cancel()
        await srv2.shutdown()


async def test_graceful_shutdown_loses_no_acknowledged_writes(tmp_path):
    """Every write the server said OK to must be present after restart."""
    aof = str(tmp_path / "ack.aof")
    port = await free_port()

    srv = MiniDBServer(Config(host="127.0.0.1", port=port, aof_path=aof))
    await srv.start()
    serving = asyncio.create_task(srv.serve_forever())

    c = await RawClient.connect(port)
    for i in range(200):
        assert await c.send("SET", f"k{i}", str(i)) == "+OK"
    await c.close()

    serving.cancel()
    await srv.shutdown()

    restored = MiniDBServer(Config(host="127.0.0.1", port=0, aof_path=aof))
    restored.load()

    assert len(restored.store) == 200
    assert restored.store.get(b"k199") == b"199"


async def test_no_persistence_when_aof_disabled(tmp_path):
    port = await free_port()
    srv = MiniDBServer(
        Config(host="127.0.0.1", port=port, aof_path=None, aof_enabled=False)
    )
    await srv.start()
    serving = asyncio.create_task(srv.serve_forever())

    c = await RawClient.connect(port)
    await c.send("SET", "ephemeral", "v")
    await c.close()

    serving.cancel()
    await srv.shutdown()

    assert list(tmp_path.iterdir()) == []


# ---------------------------------------------------------------- shutdown


async def test_shutdown_stops_accepting_connections(tmp_path):
    port = await free_port()
    srv = MiniDBServer(Config(host="127.0.0.1", port=port,
                              aof_path=str(tmp_path / "s.aof")))
    await srv.start()
    serving = asyncio.create_task(srv.serve_forever())

    c = await RawClient.connect(port)
    assert await c.send("PING") == "+PONG"
    await c.close()

    serving.cancel()
    await srv.shutdown()

    with pytest.raises((ConnectionRefusedError, OSError)):
        await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", port), timeout=2
        )
