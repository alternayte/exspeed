"""Idempotency keys."""

from __future__ import annotations

import os
import time

__all__ = ["new_msg_id"]


def new_msg_id() -> str:
    """A time-ordered UUIDv7 string, for use as a publish ``msg_id``.

    48 bits of Unix milliseconds, the version nibble 7, 12 random bits, the
    variant bits ``10`` and 62 random bits. IDs generated later sort after
    earlier ones (at millisecond granularity).
    """
    ms = time.time_ns() // 1_000_000
    rand = os.urandom(10)
    rand_a = ((rand[0] << 8) | rand[1]) & 0x0FFF
    rand_b = int.from_bytes(rand[2:], "big") & ((1 << 62) - 1)
    value = (ms & ((1 << 48) - 1)) << 80 | 0x7 << 76 | rand_a << 64 | 0b10 << 62 | rand_b
    h = f"{value:032x}"
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"
