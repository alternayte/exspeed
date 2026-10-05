from __future__ import annotations

import warnings
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from harness import SERVER_BIN, SKIP_REASON, ExspeedServer

from exspeed import ExspeedClient


def pytest_report_header(config: pytest.Config) -> str:
    return f"exspeed e2e server: {SERVER_BIN}" if SERVER_BIN else f"[exspeed] {SKIP_REASON}"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if SERVER_BIN is not None:
        return
    warnings.warn(SKIP_REASON or "e2e tests skipped", stacklevel=1)
    marker = pytest.mark.skip(reason=SKIP_REASON or "no server binary")
    e2e_dir = Path(__file__).parent
    for item in items:
        if e2e_dir in Path(str(item.fspath)).parents:
            item.add_marker(marker)


@pytest.fixture(scope="module")
def exspeed_server() -> Iterator[ExspeedServer]:
    """One server per test module."""
    s = ExspeedServer.start()
    try:
        yield s
    finally:
        s.stop()


@pytest.fixture
async def client(exspeed_server: ExspeedServer) -> AsyncIterator[ExspeedClient]:
    c = await exspeed_server.connect(client_id="e2e")
    try:
        yield c
    finally:
        await c.close()
