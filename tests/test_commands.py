"""Command semantics, tested by calling dispatch directly — no sockets."""

import pytest

from minidb.commands import dispatch
from minidb.store import Store
from tests.test_store import FakeClock


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def store(clock):
    return Store(capacity=100, clock=clock)


def run(store, *parts) -> bytes:
    """Dispatch a command written as strings; return the raw reply."""
    args = [p.encode() if isinstance(p, str) else p for p in parts]
    reply, _ = dispatch(store, args)
    return reply


def persisted(store, *parts) -> bool:
    args = [p.encode() if isinstance(p, str) else p for p in parts]
    _, should_persist = dispatch(store, args)
    return should_persist


# --------------------------------------------------------------- connection


def test_ping(store):
    assert run(store, "PING") == b"+PONG\r\n"


def test_ping_with_message(store):
    assert run(store, "PING", "hello") == b"$5\r\nhello\r\n"


def test_echo(store):
    assert run(store, "ECHO", "hi") == b"$2\r\nhi\r\n"


def test_command_docs_returns_empty_array(store):
    """redis-cli sends COMMAND DOCS on connect; it must not error."""
    assert run(store, "COMMAND", "DOCS") == b"*0\r\n"


def test_hello_rejected_so_clients_use_resp2(store):
    assert run(store, "HELLO", "3").startswith(b"-NOPROTO")


def test_select_zero_ok_others_rejected(store):
    assert run(store, "SELECT", "0") == b"+OK\r\n"
    assert run(store, "SELECT", "1").startswith(b"-ERR")


# ------------------------------------------------------------------ strings


def test_set_and_get(store):
    assert run(store, "SET", "k", "v") == b"+OK\r\n"
    assert run(store, "GET", "k") == b"$1\r\nv\r\n"


def test_get_missing_returns_nil(store):
    assert run(store, "GET", "nope") == b"$-1\r\n"


def test_set_with_ex(store, clock):
    run(store, "SET", "k", "v", "EX", "10")
    assert run(store, "TTL", "k") == b":10\r\n"

    clock.advance(11)
    assert run(store, "GET", "k") == b"$-1\r\n"


def test_set_with_px(store, clock):
    run(store, "SET", "k", "v", "PX", "500")
    clock.advance(0.6)
    assert run(store, "GET", "k") == b"$-1\r\n"


def test_set_nx_only_when_absent(store):
    assert run(store, "SET", "k", "first", "NX") == b"+OK\r\n"
    assert run(store, "SET", "k", "second", "NX") == b"$-1\r\n"
    assert run(store, "GET", "k") == b"$5\r\nfirst\r\n"


def test_set_xx_only_when_present(store):
    assert run(store, "SET", "k", "v", "XX") == b"$-1\r\n"

    run(store, "SET", "k", "v")
    assert run(store, "SET", "k", "updated", "XX") == b"+OK\r\n"
    assert run(store, "GET", "k") == b"$7\r\nupdated\r\n"


def test_set_nx_and_xx_together_is_an_error(store):
    assert run(store, "SET", "k", "v", "NX", "XX").startswith(b"-ERR")


def test_set_rejects_bad_expiry(store):
    assert run(store, "SET", "k", "v", "EX", "abc").startswith(b"-ERR")
    assert run(store, "SET", "k", "v", "EX", "0").startswith(b"-ERR")
    assert run(store, "SET", "k", "v", "EX", "-5").startswith(b"-ERR")


def test_set_rejects_unknown_option(store):
    assert run(store, "SET", "k", "v", "BOGUS").startswith(b"-ERR")


def test_setex(store, clock):
    assert run(store, "SETEX", "k", "10", "v") == b"+OK\r\n"
    assert run(store, "TTL", "k") == b":10\r\n"


def test_binary_safe_values(store):
    """Values are opaque bytes; no encoding assumptions."""
    blob = b"\x00\xff\r\nbinary"
    run(store, "SET", b"k", blob)
    assert run(store, "GET", b"k") == b"$" + str(len(blob)).encode() + b"\r\n" + blob + b"\r\n"


def test_mget(store):
    run(store, "SET", "a", "1")
    run(store, "SET", "b", "2")
    assert run(store, "MGET", "a", "b", "missing") == \
        b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n"


def test_del_counts_removed(store):
    run(store, "SET", "a", "1")
    run(store, "SET", "b", "2")
    assert run(store, "DEL", "a", "b", "nope") == b":2\r\n"


def test_exists_counts_present(store):
    run(store, "SET", "a", "1")
    assert run(store, "EXISTS", "a", "nope") == b":1\r\n"


# ------------------------------------------------------------------- expiry


def test_ttl_conventions(store):
    assert run(store, "TTL", "missing") == b":-2\r\n"

    run(store, "SET", "k", "v")
    assert run(store, "TTL", "k") == b":-1\r\n"

    run(store, "EXPIRE", "k", "30")
    assert run(store, "TTL", "k") == b":30\r\n"


def test_expire_returns_zero_for_missing(store):
    assert run(store, "EXPIRE", "nope", "10") == b":0\r\n"


def test_persist(store):
    run(store, "SET", "k", "v", "EX", "10")
    assert run(store, "PERSIST", "k") == b":1\r\n"
    assert run(store, "TTL", "k") == b":-1\r\n"
    assert run(store, "PERSIST", "k") == b":0\r\n"


# ----------------------------------------------------------------- keyspace


def test_keys_and_dbsize(store):
    run(store, "SET", "user:1", "a")
    run(store, "SET", "user:2", "b")
    run(store, "SET", "other", "c")

    assert run(store, "DBSIZE") == b":3\r\n"

    reply = run(store, "KEYS", "user:*")
    assert reply.startswith(b"*2\r\n")
    assert b"user:1" in reply and b"user:2" in reply


def test_flushall(store):
    run(store, "SET", "a", "1")
    assert run(store, "FLUSHALL") == b"+OK\r\n"
    assert run(store, "DBSIZE") == b":0\r\n"


def test_info_contains_stats(store):
    run(store, "SET", "a", "1")
    run(store, "GET", "a")
    run(store, "GET", "missing")

    info = run(store, "INFO")
    assert b"keyspace_hits:1" in info
    assert b"keyspace_misses:1" in info
    assert b"keys:1" in info


# ------------------------------------------------------------ error handling


def test_unknown_command(store):
    assert run(store, "NOSUCHCOMMAND").startswith(b"-ERR unknown command")


def test_wrong_arity_fixed(store):
    assert run(store, "GET").startswith(b"-ERR wrong number of arguments")
    assert run(store, "GET", "a", "b").startswith(b"-ERR wrong number of arguments")


def test_wrong_arity_variadic(store):
    assert run(store, "DEL").startswith(b"-ERR wrong number of arguments")


def test_command_names_are_case_insensitive(store):
    assert run(store, "set", "k", "v") == b"+OK\r\n"
    assert run(store, "GeT", "k") == b"$1\r\nv\r\n"


def test_empty_command_is_noop(store):
    reply, persist = dispatch(store, [])
    assert reply == b""
    assert persist is False


# ------------------------------------------------- what gets written to disk


def test_writes_are_flagged_for_persistence(store):
    assert persisted(store, "SET", "k", "v") is True
    assert persisted(store, "DEL", "k") is True
    assert persisted(store, "FLUSHALL") is True


def test_reads_are_not_persisted(store):
    run(store, "SET", "k", "v")
    assert persisted(store, "GET", "k") is False
    assert persisted(store, "TTL", "k") is False
    assert persisted(store, "KEYS", "*") is False


def test_failed_writes_are_not_persisted(store):
    """A rejected command must never reach the log.

    Replaying a command that originally failed would produce state that never
    actually existed.
    """
    assert persisted(store, "SET", "k", "v", "EX", "bad") is False
    assert persisted(store, "EXPIRE", "k", "notanumber") is False


def test_set_nx_that_did_nothing_is_not_persisted(store):
    """NX rejected by an existing key changed nothing, so nothing to log."""
    run(store, "SET", "k", "first")
    assert persisted(store, "SET", "k", "second", "NX") is False
