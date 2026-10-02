"""A node's link to the central server, with a network you can switch off.

In production the link is the internet; here it is a pair of PostgreSQL
connections plus a switch. Taking a node offline closes its connections, which
also drops its LISTEN subscription: notifications sent meanwhile are lost for
good, exactly like a real push channel. That is why the catch-up layer exists.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, TypeVar

import psycopg
from psycopg.rows import dict_row

from .errors import NetworkError

CHANNEL = "possync_changes"
T = TypeVar("T")


class Link:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.online = True
        self._conn: psycopg.Connection | None = None
        self._listen_conn: psycopg.Connection | None = None
        # Fault injection: the server commits, but the reply never arrives.
        self.lose_next_reply = False

    # ── Network switch ───────────────────────────────────────

    def set_online(self, online: bool) -> None:
        self.online = online
        if not online:
            self.close()

    def close(self) -> None:
        for conn in (self._conn, self._listen_conn):
            if conn is not None and not conn.closed:
                conn.close()
        self._conn = None
        self._listen_conn = None

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

    # ── Change notifications ─────────────────────────────────

    def listen(self) -> None:
        """Subscribe to change notifications (best effort)."""
        if not self.online:
            raise NetworkError("node is offline")
        if self._listen_conn is None or self._listen_conn.closed:
            try:
                self._listen_conn = psycopg.connect(self.dsn, autocommit=True)
                self._listen_conn.execute(f"LISTEN {CHANNEL}")
            except psycopg.OperationalError as exc:
                self._listen_conn = None
                raise NetworkError(str(exc)) from exc

    @property
    def listening(self) -> bool:
        return self._listen_conn is not None and not self._listen_conn.closed

    def drain_notifications(self, timeout: float = 0.05) -> list[dict]:
        """Return the notifications received so far (empty when not listening)."""
        if not self.online or not self.listening:
            return []
        received: list[dict] = []
        try:
            for notify in self._listen_conn.notifies(timeout=timeout):
                received.append(json.loads(notify.payload))
        except psycopg.OperationalError:
            self._listen_conn = None
        return received
