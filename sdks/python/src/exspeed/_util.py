"""Internal helpers."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine, Generator
from typing import Any, Generic, TypeVar

T = TypeVar("T")


async def wait_future(fut: asyncio.Future[T], timeout: float | None) -> bool:
    """Wait for ``fut`` up to ``timeout`` seconds without cancelling it; True when it is done."""
    if fut.done():
        return True
    if timeout is None:
        await asyncio.wait({fut})
        return True
    if timeout <= 0:
        return False
    done, _ = await asyncio.wait({fut}, timeout=timeout)
    return bool(done)


class AwaitableContext(Generic[T]):
    """Wrap a coroutine so it can be awaited, or used with ``async with``.

    ``await x`` returns the result. ``async with x as r`` awaits it and, on
    exit, awaits ``closer(r)``.
    """

    __slots__ = ("_closer", "_coro", "_result")

    def __init__(self, coro: Coroutine[Any, Any, T], closer: Callable[[T], Awaitable[object]]) -> None:
        self._coro = coro
        self._closer = closer
        self._result: T | None = None

    def __await__(self) -> Generator[Any, None, T]:
        return self._coro.__await__()

    async def __aenter__(self) -> T:
        self._result = await self._coro
        return self._result

    async def __aexit__(self, *exc: object) -> None:
        if self._result is not None:
            await self._closer(self._result)
