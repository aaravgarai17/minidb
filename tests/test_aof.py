"""Persistence: logging, replay, crash tolerance, compaction."""

import os

import pytest

from minidb.aof import AOF, FSYNC_ALWAYS, FSYNC_NO
from minidb.commands import dispatch
from minidb.store import Store


@pytest.fixture
def aof_path(tmp_path):
    return tmp_path / "test.aof"


@pytest.fixture
def aof(aof_path):
    a = AOF(aof_path, fsync_policy=FSYNC_NO)
    a.open()
    yield a
    a.close()


# ------------------------------------------------------------------ logging


def test_write_commands_are_logged(aof):
    aof.log([b"SET", b"k", b"v"])
    assert aof.writes_logged == 1


def test_read_commands_are_not_logged(aof):
    """Replaying a GET would do nothing and would bloat the file."""
    for cmd in ([b"GET", b"k"], [b"TTL", b"k"], [b"KEYS", b"*"], [b"INFO"]):
        aof.log(cmd)
    assert aof.writes_logged == 0


def test_logged_bytes_are_resp(aof, aof_path):
    """The file holds exactly what a client would have sent.

    Keeping one format means replay reuses the live command path instead of a
    parallel deserializer that could drift out of sync.
    """
    aof.log([b"SET", b"k", b"v"])
    aof.close()
    assert aof_path.read_bytes() == b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"


def test_disabled_aof_writes_nothing(tmp_path):
    a = AOF(tmp_path / "x.aof", enabled=False)
    a.open()
    a.log([b"SET", b"k", b"v"])
    a.close()
    assert not (tmp_path / "x.aof").exists()


def test_none_path_disables_persistence():
    a = AOF(None)
    a.open()
    a.log([b"SET", b"k", b"v"])
    a.close()
    assert a.enabled is False


# ------------------------------------------------------------------- replay


def test_replay_yields_commands_in_order(aof):
    aof.log([b"SET", b"a", b"1"])
    aof.log([b"SET", b"b", b"2"])
    aof.log([b"DEL", b"a"])
    aof.close()

    assert list(aof.replay()) == [
        [b"SET", b"a", b"1"],
        [b"SET", b"b", b"2"],
        [b"DEL", b"a"],
    ]


def test_replay_of_missing_file_is_empty(tmp_path):
    assert list(AOF(tmp_path / "nope.aof").replay()) == []


def test_replay_rebuilds_state(aof):
    """The end-to-end durability guarantee, in miniature."""
    original = Store()
    for cmd in (
        [b"SET", b"a", b"1"],
        [b"SET", b"b", b"2"],
        [b"SET", b"a", b"updated"],
        [b"DEL", b"b"],
    ):
        dispatch(original, cmd)
        aof.log(cmd)
    aof.close()

    restored = Store()
    for cmd in aof.replay():
        dispatch(restored, cmd)

    assert restored.get(b"a") == b"updated"
    assert restored.get(b"b") is None
    assert len(restored) == len(original)


def test_replay_preserves_binary_values(aof):
    blob = b"\x00\xff\r\n\x01binary"
    aof.log([b"SET", b"k", blob])
    aof.close()

    s = Store()
    for cmd in aof.replay():
        dispatch(s, cmd)
    assert s.get(b"k") == blob


def test_replay_handles_empty_values(aof):
    aof.log([b"SET", b"k", b""])
    aof.close()

    s = Store()
    for cmd in aof.replay():
        dispatch(s, cmd)
    assert s.get(b"k") == b""


# --------------------------------------------------------- crash tolerance


def test_truncated_tail_is_skipped_not_fatal(aof, aof_path):
    """A half-written final command is the normal result of a crash.

    Every complete command before it is still valid. Rejecting the whole file
    would turn a minor event into total data loss.
    """
    aof.log([b"SET", b"a", b"1"])
    aof.log([b"SET", b"b", b"2"])
    aof.close()

    data = aof_path.read_bytes()
    aof_path.write_bytes(data[:-6])            # chop the tail mid-command

    recovered = list(AOF(aof_path).replay())
    assert recovered == [[b"SET", b"a", b"1"]]


def test_garbage_tail_is_skipped(aof, aof_path):
    aof.log([b"SET", b"a", b"1"])
    aof.close()

    with open(aof_path, "ab") as fh:
        fh.write(b"\x00\x01garbage")

    assert list(AOF(aof_path).replay()) == [[b"SET", b"a", b"1"]]


def test_empty_file_replays_cleanly(aof_path):
    aof_path.write_bytes(b"")
    assert list(AOF(aof_path).replay()) == []


# ------------------------------------------------------------------ fsync


def test_fsync_always_syncs_every_write(aof_path):
    a = AOF(aof_path, fsync_policy=FSYNC_ALWAYS)
    a.open()
    for i in range(5):
        a.log([b"SET", f"k{i}".encode(), b"v"])
    syncs_during = a.fsyncs
    a.close()

    assert syncs_during == 5


def test_fsync_no_defers_to_the_os(aof_path):
    a = AOF(aof_path, fsync_policy=FSYNC_NO)
    a.open()
    for i in range(5):
        a.log([b"SET", f"k{i}".encode(), b"v"])
    syncs_during = a.fsyncs
    a.close()

    assert syncs_during == 0


def test_close_always_syncs(aof_path):
    """Why a clean shutdown loses nothing even under a lazy fsync policy."""
    a = AOF(aof_path, fsync_policy=FSYNC_NO)
    a.open()
    a.log([b"SET", b"k", b"v"])
    a.close()

    assert a.fsyncs == 1
    assert list(AOF(aof_path).replay()) == [[b"SET", b"k", b"v"]]


# -------------------------------------------------------------- compaction


def test_rewrite_shrinks_a_repetitive_log(aof, aof_path):
    """The log records history; compaction reduces it to state.

    A key written a thousand times contributes a thousand entries, of which
    only the last matters.
    """
    for i in range(500):
        aof.log([b"SET", b"counter", str(i).encode()])
    aof.close()

    before = aof_path.stat().st_size
    AOF(aof_path).rewrite([[b"SET", b"counter", b"499"]])
    after = aof_path.stat().st_size

    assert after < before / 10
    assert list(AOF(aof_path).replay()) == [[b"SET", b"counter", b"499"]]


def test_rewrite_preserves_state(aof, aof_path):
    for i in range(50):
        aof.log([b"SET", f"k{i}".encode(), str(i).encode()])
    aof.close()

    live = Store()
    for cmd in AOF(aof_path).replay():
        dispatch(live, cmd)

    snapshot = [[b"SET", k, live.get(k)] for k in live.keys("*")]
    AOF(aof_path).rewrite(snapshot)

    restored = Store()
    for cmd in AOF(aof_path).replay():
        dispatch(restored, cmd)

    assert len(restored) == 50
    assert restored.get(b"k7") == b"7"


def test_rewrite_is_atomic(aof, aof_path):
    """Written to a temp file and renamed, so no partial file is ever visible."""
    aof.log([b"SET", b"a", b"1"])
    aof.close()

    AOF(aof_path).rewrite([[b"SET", b"b", b"2"]])

    leftovers = [p for p in aof_path.parent.iterdir() if ".rewrite" in p.name]
    assert leftovers == []
    assert list(AOF(aof_path).replay()) == [[b"SET", b"b", b"2"]]


def test_rewrite_while_open_keeps_the_file_usable(aof_path):
    a = AOF(aof_path, fsync_policy=FSYNC_NO)
    a.open()
    a.log([b"SET", b"a", b"1"])

    a.rewrite([[b"SET", b"a", b"compacted"]])
    a.log([b"SET", b"b", b"2"])           # must still be writable afterwards
    a.close()

    assert list(AOF(aof_path).replay()) == [
        [b"SET", b"a", b"compacted"],
        [b"SET", b"b", b"2"],
    ]


def test_stats(aof):
    aof.log([b"SET", b"k", b"v"])
    s = aof.stats()
    assert s["enabled"] is True
    assert s["writes_logged"] == 1
    assert s["fsync_policy"] == FSYNC_NO
