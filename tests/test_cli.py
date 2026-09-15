"""Tests for the CLI client.

This module shipped untested — 117 statements at 0% coverage — which is a poor
place to leave the one piece of the project a user actually types into. The
reply parser in particular is easy to get subtly wrong: it has to distinguish a
nil from an empty string, and a nested array from a flat one.

A fake socket stands in for a real connection, so these run with no server.
"""

import pytest

from minidb.cli import Client, Error, format_reply


class FakeSocket:
    """Returns a scripted byte stream, in whatever chunk sizes we choose.

    Chunking is configurable because the client must handle a reply arriving
    split across several `recv` calls — TCP makes no promise that one send is
    one receive, and a parser that assumes otherwise breaks under load rather
    than in tests.
    """

    def __init__(self, data: bytes, chunk_size: int = 65536):
        self.data = data
        self.chunk_size = chunk_size
        self.pos = 0
        self.sent = b""

    def recv(self, _bufsize: int) -> bytes:
        chunk = self.data[self.pos : self.pos + self.chunk_size]
        self.pos += len(chunk)
        return chunk

    def sendall(self, data: bytes) -> None:
        self.sent += data

    def close(self) -> None:
        pass


def client_for(data: bytes, chunk_size: int = 65536) -> Client:
    c = Client()
    c.sock = FakeSocket(data, chunk_size)
    return c


# ------------------------------------------------------------- reply parsing


def test_simple_string():
    assert client_for(b"+OK\r\n")._read_reply() == "OK"


def test_error_reply_is_distinguishable():
    """An error must not look like an ordinary string reply."""
    reply = client_for(b"-ERR unknown command\r\n")._read_reply()
    assert isinstance(reply, Error)
    assert reply == "ERR unknown command"


def test_integer():
    assert client_for(b":42\r\n")._read_reply() == 42


def test_negative_integer():
    assert client_for(b":-2\r\n")._read_reply() == -2


def test_bulk_string():
    assert client_for(b"$5\r\nhello\r\n")._read_reply() == "hello"


def test_nil_is_not_empty_string():
    """`$-1` means no such key; `$0` means the key holds an empty string.

    Collapsing them would make a missing key indistinguishable from an empty
    value, which is exactly the distinction a user needs.
    """
    assert client_for(b"$-1\r\n")._read_reply() is None
    assert client_for(b"$0\r\n\r\n")._read_reply() == ""


def test_bulk_string_containing_crlf():
    """Bulk strings are length-prefixed, so payloads may contain CRLF.

    The payload here is 5 bytes: 'a', CR, LF, 'b', '!'. A parser that scanned
    for the next CRLF instead of honouring the declared length would truncate
    this to 'a'.
    """
    assert client_for(b"$5\r\na\r\nb!\r\n")._read_reply() == "a\r\nb!"


def test_array():
    assert client_for(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")._read_reply() == ["foo", "bar"]


def test_empty_array():
    assert client_for(b"*0\r\n")._read_reply() == []


def test_null_array():
    assert client_for(b"*-1\r\n")._read_reply() is None


def test_array_with_mixed_types():
    data = b"*3\r\n$1\r\na\r\n:7\r\n$-1\r\n"
    assert client_for(data)._read_reply() == ["a", 7, None]


def test_nested_array():
    data = b"*2\r\n*1\r\n$1\r\na\r\n*1\r\n$1\r\nb\r\n"
    assert client_for(data)._read_reply() == [["a"], ["b"]]


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 7])
def test_reply_split_across_multiple_recv_calls(chunk_size):
    """TCP does not promise one send equals one receive.

    A parser that assumes a whole reply arrives at once works in tests and
    fails under real network conditions, so it is tested at several chunk
    sizes including one byte at a time.
    """
    data = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    assert client_for(data, chunk_size)._read_reply() == ["hello", "world"]


def test_closed_connection_raises():
    """An empty recv means the peer hung up, and must not loop forever."""
    with pytest.raises(ConnectionError):
        client_for(b"")._read_reply()


def test_truncated_reply_raises():
    with pytest.raises(ConnectionError):
        client_for(b"$10\r\nshort")._read_reply()


def test_unknown_reply_type_raises():
    with pytest.raises(ConnectionError):
        client_for(b"?what\r\n")._read_reply()


# ---------------------------------------------------------------- sending


def test_send_encodes_as_resp_array():
    c = client_for(b"+OK\r\n")
    c.send(["SET", "key", "value"])
    assert c.sock.sent == b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"


def test_send_returns_the_parsed_reply():
    c = client_for(b"$3\r\nabc\r\n")
    assert c.send(["GET", "k"]) == "abc"


# -------------------------------------------------------------- formatting


def test_format_string_is_quoted():
    assert format_reply("hello") == '"hello"'


def test_format_integer():
    assert format_reply(42) == "(integer) 42"


def test_format_nil():
    assert format_reply(None) == "(nil)"


def test_format_error():
    assert format_reply(Error("ERR nope")) == "(error) ERR nope"


def test_format_empty_array():
    assert format_reply([]) == "(empty array)"


def test_format_array_is_numbered_from_one():
    out = format_reply(["a", "b"])
    assert out.startswith("1) ")
    assert "2) " in out


def test_format_multiline_info_output_is_not_quoted():
    """INFO returns a CRLF-separated block; quoting it would be unreadable."""
    out = format_reply("# Server\r\nkeys:3\r\n")
    assert '"' not in out
    assert "# Server" in out
    assert "keys:3" in out
