"""RESP — the Redis Serialization Protocol.

Why implement RESP rather than inventing a line protocol
--------------------------------------------------------
A custom protocol would have been marginally simpler, but speaking RESP means
every existing Redis tool works against this server unchanged: `redis-cli`
connects to it, `redis-benchmark` measures it, and the official Python client
(`redis-py`) drives it. That turns "trust me, it works" into something anyone
can check in ten seconds with software they already have. It also makes the
benchmark against real Redis an apples-to-apples comparison, since both sides
are driven by the identical client.

The format
----------
Five types, each identified by its first byte and terminated by CRLF:

    +OK\\r\\n                          simple string
    -ERR unknown command\\r\\n         error
    :42\\r\\n                          integer
    $5\\r\\nhello\\r\\n                  bulk string (length-prefixed, binary safe)
    $-1\\r\\n                          null bulk string
    *2\\r\\n$3\\r\\nGET\\r\\n$1\\r\\na\\r\\n   array

Clients send commands as arrays of bulk strings; servers reply with whichever
type fits. Bulk strings carry an explicit byte count, so values may contain
CRLF, NUL, or arbitrary binary without escaping.
"""

from __future__ import annotations

import asyncio
from typing import Optional, Sequence

CRLF = b"\r\n"

# Guardrails against a malicious or broken client claiming an enormous payload.
# Without these, `$999999999999` would have us try to allocate a terabyte.
MAX_BULK_SIZE = 512 * 1024 * 1024      # 512 MB, same ceiling as Redis
MAX_ARRAY_ELEMENTS = 1024 * 1024


class ProtocolError(Exception):
    """Raised when input cannot be parsed as RESP. The connection is closed."""


# --------------------------------------------------------------------- encode


def encode_simple(text: str) -> bytes:
    """Simple string: for short, known-safe replies like OK or PONG."""
    return b"+" + text.encode() + CRLF


def encode_error(message: str) -> bytes:
    return b"-" + message.encode() + CRLF


def encode_integer(value: int) -> bytes:
    return b":" + str(value).encode() + CRLF


def encode_bulk(value: Optional[bytes | str]) -> bytes:
    """Bulk string, or the null bulk string when value is None.

    Null is distinct from empty: `$-1` means "no such key", `$0` means "the key
    exists and holds an empty string". Collapsing them would make it impossible
    for a client to tell a missing key from an empty one.
    """
    if value is None:
        return b"$-1" + CRLF
    if isinstance(value, str):
        value = value.encode()
    return b"$" + str(len(value)).encode() + CRLF + value + CRLF


def encode_array(items: Optional[Sequence]) -> bytes:
    if items is None:
        return b"*-1" + CRLF

    out = [b"*" + str(len(items)).encode() + CRLF]
    for item in items:
        if isinstance(item, bytes) or isinstance(item, str) or item is None:
            out.append(encode_bulk(item))
        elif isinstance(item, int):
            out.append(encode_integer(item))
        elif isinstance(item, (list, tuple)):
            out.append(encode_array(item))
        else:
            out.append(encode_bulk(str(item)))
    return b"".join(out)


# --------------------------------------------------------------------- decode


async def read_command(reader: asyncio.StreamReader) -> Optional[list[bytes]]:
    """Read one complete command from the stream.

    Returns the argument list, or None at end of stream.

    Accepts both forms a server sees in practice:

      * **RESP arrays** — what every real client library sends.
      * **Inline commands** — bare text like `GET foo\\r\\n`, which is what you
        get from `telnet` or `nc`. Redis supports these for exactly that
        reason, and so does this: being able to debug the server with netcat
        when a client library is misbehaving is genuinely useful.
    """
    line = await reader.readline()
    if not line:
        return None                       # clean EOF, client hung up

    line = line.rstrip(b"\r\n")

    if not line:
        return []                         # blank line: ignore, keep connection

    if line[:1] != b"*":
        return _parse_inline(line)

    return await _parse_array(reader, line)


def _parse_inline(line: bytes) -> list[bytes]:
    """Whitespace-split a bare command line."""
    return [part for part in line.split() if part]


async def _parse_array(
    reader: asyncio.StreamReader, header: bytes
) -> list[bytes]:
    try:
        count = int(header[1:])
    except ValueError:
        raise ProtocolError("invalid multibulk length")

    if count <= 0:
        return []                         # empty or null array
    if count > MAX_ARRAY_ELEMENTS:
        raise ProtocolError("invalid multibulk length")

    args: list[bytes] = []
    for _ in range(count):
        arg_header = await reader.readline()
        if not arg_header:
            raise ProtocolError("unexpected end of stream")

        arg_header = arg_header.rstrip(b"\r\n")
        if arg_header[:1] != b"$":
            raise ProtocolError(
                f"expected '$', got '{arg_header[:1].decode(errors='replace')}'"
            )

        try:
            length = int(arg_header[1:])
        except ValueError:
            raise ProtocolError("invalid bulk length")

        if length == -1:
            args.append(b"")              # null inside a command: treat as empty
            continue
        if length < 0 or length > MAX_BULK_SIZE:
            raise ProtocolError("invalid bulk length")

        try:
            # +2 consumes the trailing CRLF along with the payload.
            payload = await reader.readexactly(length + 2)
        except asyncio.IncompleteReadError:
            raise ProtocolError("unexpected end of stream")

        args.append(payload[:-2])

    return args
