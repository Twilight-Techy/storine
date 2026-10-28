"""Errors raised by the Storine client. Each says what happened and what to do about it."""

from __future__ import annotations

from typing import Optional


class StorineError(Exception):
    """Base class of every Storine error."""

    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.message = message
        self.status = status


class NotFoundError(StorineError):
    """The document does not exist (e.g. `update` of a missing key)."""


class ConflictError(StorineError):
    """A condition did not hold: the document already exists, does not exist, or changed.

    `write_index` is the position of the failing write in the request and `current_version` the
    document's version now (`None` if it does not exist). Re-read and decide again.
    """

    def __init__(self, message: str, write_index: Optional[int] = None, current_version: Optional[int] = None) -> None:
        super().__init__(message, 0x04)
        self.write_index = write_index
        self.current_version = current_version


class InvalidError(StorineError):
    """The request was rejected as invalid (bad value, type mismatch, bound violated). Do not retry."""


class UnauthorizedError(StorineError):
    """Authentication failed or the token's role does not allow this operation."""


class OverloadedError(StorineError):
    """The server is temporarily overloaded. Retry later."""


class RateLimitedError(OverloadedError):
    """This connection exceeded its rate limit. Retry later."""


class UnavailableError(StorineError):
    """No server (or no cluster leader) could be reached in time. Retry later."""


class ServerError(StorineError):
    """The server failed internally (I/O error...). The message has the details."""
