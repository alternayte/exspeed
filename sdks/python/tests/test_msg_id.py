from __future__ import annotations

import re
import time
import uuid

from exspeed import new_msg_id


def test_returns_unique_values() -> None:
    assert len({new_msg_id() for _ in range(100)}) == 100


def test_matches_uuidv7_format() -> None:
    mid = new_msg_id()
    assert re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", mid)
    u = uuid.UUID(mid)
    assert u.version == 7
    assert u.variant == uuid.RFC_4122
    assert len(mid) == 36


def test_embeds_the_current_millisecond_time() -> None:
    before = time.time_ns() // 1_000_000
    ms = int(new_msg_id().replace("-", "")[:12], 16)
    after = time.time_ns() // 1_000_000
    assert before <= ms <= after


def test_later_generated_ids_sort_after_earlier_ones() -> None:
    a = new_msg_id()
    time.sleep(0.002)
    assert a < new_msg_id()
