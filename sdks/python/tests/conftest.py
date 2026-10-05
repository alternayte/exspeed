from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from fake_server import FakeServer


@pytest.fixture
async def server() -> AsyncIterator[FakeServer]:
    s = await FakeServer.start()
    try:
        yield s
    finally:
        await s.close()
