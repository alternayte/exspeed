"""Exceptions raised by the Exspeed client.

Every exception derives from :class:`ExspeedError`. :class:`ConnectionError`
and :class:`TimeoutError` also derive from the built-in exceptions of the same
name, so ``except TimeoutError`` catches a request timeout too.
"""

from __future__ import annotations

import builtins
from typing import Any

__all__ = [
    "ExspeedError",
    "ServerError",
    "ConnectionError",
    "TimeoutError",
    "ProtocolError",
]


class ExspeedError(Exception):
    """Base class of every error this client raises."""


class ServerError(ExspeedError):
    """The server answered a request with an ``Error`` frame.

    Attributes:
        code: HTTP-like status code (see :class:`exspeed.ErrorCode`), for
            example 404, 409, 429 or 503.
        message: The server's message.
        detail: The optional machine-readable JSON the server attached,
            decoded, with the server's snake_case keys: ``{"leader": "h:5933"}``
            (503), ``{"stored_offset": 7}`` or ``{"current_revision": 3}`` (409),
            ``{"retry_after_secs": 30}`` (429). ``None`` when absent.
    """

    code: int
    message: str
    detail: Any

    def __init__(self, code: int, message: str, detail: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail

    @property
    def leader_hint(self) -> str | None:
        """``detail["leader"]`` of a 503 "not the leader" error: the leader's client address, when known."""
        d = self.detail
        if isinstance(d, dict):
            leader = d.get("leader")
            if isinstance(leader, str):
                return leader
        return None

    def __str__(self) -> str:
        return f"ServerError {self.code}: {self.message}"

    def __repr__(self) -> str:
        return f"ServerError(code={self.code!r}, message={self.message!r}, detail={self.detail!r})"


class ConnectionError(ExspeedError, builtins.ConnectionError):
    """The connection is closed, was lost, could not be opened, or is being re-established."""

    def __init__(self, message: str) -> None:
        super().__init__(message)

    def __str__(self) -> str:
        return str(self.args[0]) if self.args else "connection error"


class TimeoutError(ExspeedError, builtins.TimeoutError):
    """No response arrived within the request timeout."""

    def __init__(self, message: str = "request timed out") -> None:
        super().__init__(message)

    def __str__(self) -> str:
        return str(self.args[0]) if self.args else "request timed out"


class ProtocolError(ExspeedError):
    """The peer sent bytes this client cannot decode, or a reply of the wrong type."""
