"""A node's link to the central server, with a network you can switch off.

In production the link is the internet; here it is a PostgreSQL connection
plus a switch. Taking a node offline closes the connection, and every call
raises NetworkError until it comes back.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

import psycopg
from psycopg.rows import dict_row

from .errors import NetworkError

T = TypeVar("T")


class Link:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.online = True
        self._conn: psycopg.Connection | None = None
        # Fault injection: the server commits, but the reply never arrives.
        self.lose_next_reply = False

    # ── Network switch ───────────────────────────────────────

    def set_online(self, online: bool) -> None:
        self.online = online
        if not online:
            self.close()

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

    # ── Request / reply ──────────────────────────────────────

    def call(self, fn: Callable[..., T], *args: Any) -> T:
        """Run a server function. Raises NetworkError when offline, when the
        connection fails, or when the reply is "lost" after the server committed."""
        if not self.online:
            raise NetworkError("node is offline")
        try:
            result = fn(self._connection(), *args)
        except psycopg.OperationalError as exc:
            self.close()
            raise NetworkError(str(exc)) from exc
        if self.lose_next_reply:
            self.lose_next_reply = False
            raise NetworkError("reply lost after the server committed")
        return result

    def _connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self.dsn, autocommit=True, row_factory=dict_row)
        return self._conn
