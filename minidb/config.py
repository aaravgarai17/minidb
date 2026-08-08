"""Server configuration, from CLI flags or environment variables."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

from minidb.aof import FSYNC_ALWAYS, FSYNC_EVERYSEC, FSYNC_NO


@dataclass
class Config:
    host: str = "0.0.0.0"
    port: int = 6380                  # not 6379, so it can run beside real Redis
    max_keys: int = 100_000
    aof_path: str | None = "data/minidb.aof"
    aof_enabled: bool = True
    fsync_policy: str = FSYNC_EVERYSEC
    expiry_interval: float = 0.1      # seconds between active-expiry cycles
    auto_rewrite_multiplier: float = 2.0
    auto_rewrite_min_size: int = 1024 * 1024   # 1 MB


def _env(name: str, default):
    return os.getenv(f"MINIDB_{name}", default)


def parse_args(argv=None) -> Config:
    p = argparse.ArgumentParser(
        prog="minidb",
        description="A Redis-compatible in-memory key-value store.",
    )
    p.add_argument("--host", default=_env("HOST", "0.0.0.0"))
    p.add_argument("--port", type=int, default=int(_env("PORT", 6380)))
    p.add_argument(
        "--max-keys",
        type=int,
        default=int(_env("MAX_KEYS", 100_000)),
        help="Capacity before LRU eviction begins.",
    )
    p.add_argument(
        "--aof",
        default=_env("AOF_PATH", "data/minidb.aof"),
        help="Append-only file path.",
    )
    p.add_argument(
        "--no-aof",
        action="store_true",
        default=_env("NO_AOF", "") not in ("", "0", "false"),
        help="Disable persistence entirely (pure in-memory).",
    )
    p.add_argument(
        "--fsync",
        choices=[FSYNC_ALWAYS, FSYNC_EVERYSEC, FSYNC_NO],
        default=_env("FSYNC", FSYNC_EVERYSEC),
        help="Durability policy. See README for the trade-off.",
    )
    args = p.parse_args(argv)

    return Config(
        host=args.host,
        port=args.port,
        max_keys=args.max_keys,
        aof_path=None if args.no_aof else args.aof,
        aof_enabled=not args.no_aof,
        fsync_policy=args.fsync,
    )
