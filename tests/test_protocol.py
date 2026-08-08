"""RESP encoding and parsing."""

import asyncio

import pytest

from minidb import protocol
from minidb.protocol import ProtocolError


def make_reader(data: bytes) -> asyncio.StreamReader:
    r = asyncio.StreamReader()
    r.feed_data(data)
    r.feed_eof()
    return r


# ------------------------------------------------------------------ encoding


def test_encode_simple_string():
    assert protocol.encode_simple("OK") == b"+OK\r\n"


def test_encode_error():
    assert protocol.encode_error("ERR nope") == b"-ERR nope\r\n"


def test_encode_integer():
    assert protocol.encode_integer(42) == b":42\r\n"
    assert protocol.encode_integer(-1) == b":-1\r\n"


def test_encode_bulk():
    assert protocol.encode_bulk(b"hello") == b"$5\r\nhello\r\n"
    assert protocol.encode_bulk("hello") == b"$5\r\nhello\r\n"


def test_encode_bulk_empty_vs_null():
    """Empty string and nil are different replies and must stay distinguishable.

    Collapsing them would make it impossible for a client to tell "key missing"
    from "key holds an empty value".
    """
    assert protocol.encode_bulk(b"") == b"$0\r\n\r\n"
    assert protocol.encode_bulk(None) == b"$-1\r\n"


def test_encode_bulk_is_binary_safe():
    """Length prefixing means payloads may contain CRLF or NUL unescaped."""
    payload = b"line1\r\nline2\x00end"
    encoded = protocol.encode_bulk(payload)
    assert encoded == b"$" + str(len(payload)).encode() + b"\r\n" + payload + b"\r\n"


def test_encode_array():
    assert protocol.encode_array([b"GET", b"k"]) == b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"


def test_encode_empty_and_null_array():
    assert protocol.encode_array([]) == b"*0\r\n"
    assert protocol.encode_array(None) == b"*-1\r\n"


def test_encode_array_mixed_types():
    out = protocol.encode_array([b"a", 5, None])
    assert out == b"*3\r\n$1\r\na\r\n:5\r\n$-1\r\n"


def test_encode_nested_array():
    out = protocol.encode_array([[b"a"], [b"b"]])
    assert out == b"*2\r\n*1\r\n$1\r\na\r\n*1\r\n$1\r\nb\r\n"


# ------------------------------------------------------------------- parsing


@pytest.mark.asyncio
async def test_parse_array_command():
    r = make_reader(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
    assert await protocol.read_command(r) == [b"GET", b"foo"]


@pytest.mark.asyncio
async def test_parse_inline_command():
    """Bare text, as sent by telnet or netcat."""
    r = make_reader(b"GET foo\r\n")
    assert await protocol.read_command(r) == [b"GET", b"foo"]


@pytest.mark.asyncio
async def test_parse_inline_collapses_extra_whitespace():
    r = make_reader(b"SET   key    value\r\n")
    assert await protocol.read_command(r) == [b"SET", b"key", b"value"]


@pytest.mark.asyncio
async def test_parse_multiple_commands_in_one_buffer():
    """Pipelined commands arriving in a single TCP segment."""
    r = make_reader(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")
    assert await protocol.read_command(r) == [b"PING"]
    assert await protocol.read_command(r) == [b"PING"]


@pytest.mark.asyncio
async def test_parse_value_containing_crlf():
    payload = b"a\r\nb"
    raw = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$" + str(len(payload)).encode() + b"\r\n" + payload + b"\r\n"
    r = make_reader(raw)
    assert await protocol.read_command(r) == [b"SET", b"k", payload]


@pytest.mark.asyncio
async def test_parse_empty_value():
    r = make_reader(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n")
    assert await protocol.read_command(r) == [b"SET", b"k", b""]


@pytest.mark.asyncio
async def test_eof_returns_none():
    r = make_reader(b"")
    assert await protocol.read_command(r) is None


@pytest.mark.asyncio
async def test_blank_line_returns_empty_list():
    r = make_reader(b"\r\n")
    assert await protocol.read_command(r) == []


@pytest.mark.asyncio
async def test_zero_length_array():
    r = make_reader(b"*0\r\n")
    assert await protocol.read_command(r) == []


@pytest.mark.asyncio
async def test_bad_multibulk_length_raises():
    r = make_reader(b"*abc\r\n")
    with pytest.raises(ProtocolError):
        await protocol.read_command(r)


@pytest.mark.asyncio
async def test_missing_dollar_prefix_raises():
    r = make_reader(b"*1\r\n#3\r\nGET\r\n")
    with pytest.raises(ProtocolError):
        await protocol.read_command(r)


@pytest.mark.asyncio
async def test_truncated_payload_raises():
    r = make_reader(b"*1\r\n$10\r\nshort\r\n")
    with pytest.raises(ProtocolError):
        await protocol.read_command(r)


@pytest.mark.asyncio
async def test_absurd_bulk_length_rejected():
    """Guards against a client claiming a payload larger than memory.

    Without a ceiling this would attempt an enormous allocation — a trivial
    denial-of-service.
    """
    r = make_reader(b"*1\r\n$999999999999\r\n")
    with pytest.raises(ProtocolError):
        await protocol.read_command(r)


@pytest.mark.asyncio
async def test_absurd_array_count_rejected():
    r = make_reader(b"*999999999\r\n")
    with pytest.raises(ProtocolError):
        await protocol.read_command(r)
