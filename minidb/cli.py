"""Interactive client — a minimal `redis-cli`.

Real `redis-cli` also works against this server (that's the point of speaking
RESP), but shipping a client makes the project self-contained: someone can
clone the repo and interact with the server without installing Redis.

Run:  python -m minidb.cli
      python -m minidb.cli --port 6380
      python -m minidb.cli GET mykey        # one-shot, then exit
"""

from __future__ import annotations

import argparse
import socket
import sys
from typing import Optional

from minidb.protocol import encode_array

# `readline` transparently upgrades input() with history and arrow keys.
try:
    import readline  # noqa: F401
except ImportError:  # pragma: no cover
    pass


class Client:
    def __init__(self, host: str = "127.0.0.1", port: int = 6380) -> None:
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
        self._buf = b""

    def connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=5)

    def close(self) -> None:
        if self.sock:
            self.sock.close()
            self.sock = None

    def send(self, parts: list[str]):
        self.sock.sendall(encode_array([p.encode() for p in parts]))
        return self._read_reply()

    # ------------------------------------------------------------ RESP reader

    def _read_line(self) -> bytes:
        while b"\r\n" not in self._buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("server closed the connection")
            self._buf += chunk

        line, self._buf = self._buf.split(b"\r\n", 1)
        return line

    def _read_exact(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("server closed the connection")
            self._buf += chunk

        data, self._buf = self._buf[:n], self._buf[n:]
        return data

    def _read_reply(self):
        line = self._read_line()
        kind, rest = line[:1], line[1:]

        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            return Error(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            length = int(rest)
            if length == -1:
                return None
            data = self._read_exact(length + 2)[:-2]
            return data.decode("utf-8", errors="replace")
        if kind == b"*":
            count = int(rest)
            if count == -1:
                return None
            return [self._read_reply() for _ in range(count)]

        raise ConnectionError(f"unexpected reply type: {kind!r}")


class Error(str):
    """An error reply, distinguishable from a normal string."""


def format_reply(reply, indent: int = 0) -> str:
    pad = "  " * indent

    if isinstance(reply, Error):
        return f"{pad}(error) {reply}"
    if reply is None:
        return f"{pad}(nil)"
    if isinstance(reply, bool):
        return f"{pad}{int(reply)}"
    if isinstance(reply, int):
        return f"{pad}(integer) {reply}"
    if isinstance(reply, list):
        if not reply:
            return f"{pad}(empty array)"
        return "\n".join(
            f"{pad}{i + 1}) {format_reply(item, 0)}"
            for i, item in enumerate(reply)
        )
    text = str(reply)
    if "\r\n" in text:                    # INFO output
        return "\n".join(f"{pad}{ln}" for ln in text.split("\r\n"))
    return f'{pad}"{text}"'


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="minidb-cli")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6380)
    p.add_argument("command", nargs="*", help="Run one command and exit.")
    args = p.parse_args(argv)

    client = Client(args.host, args.port)
    try:
        client.connect()
    except OSError as exc:
        print(f"could not connect to {args.host}:{args.port} — {exc}",
              file=sys.stderr)
        return 1

    try:
        if args.command:
            reply = client.send(args.command)
            print(format_reply(reply))
            return 1 if isinstance(reply, Error) else 0

        print(f"connected to minidb at {args.host}:{args.port}")
        print("type commands, or 'exit' to quit\n")

        while True:
            try:
                raw = input(f"{args.host}:{args.port}> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not raw:
                continue
            if raw.lower() in ("exit", "quit"):
                break

            try:
                print(format_reply(client.send(raw.split())))
            except ConnectionError as exc:
                print(f"(error) {exc}", file=sys.stderr)
                break
    finally:
        client.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
