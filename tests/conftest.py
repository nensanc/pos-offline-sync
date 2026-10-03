"""Shared fixtures. The tests need the PostgreSQL from docker-compose:

docker compose up -d db
pytest
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator

import psycopg
import pytest
from psycopg.rows import dict_row

from possync import server
from possync.node import Node
from possync.transport import Link

DSN = os.environ.get("DATABASE_URL", "postgresql://possync:possync@localhost:5433/possync")


@pytest.fixture
def admin() -> Iterator[psycopg.Connection]:
    """A direct connection to the server with a clean schema."""
    try:
        conn = psycopg.connect(DSN, autocommit=True, row_factory=dict_row, connect_timeout=5)
    except psycopg.OperationalError as exc:
        pytest.fail(f"PostgreSQL is not reachable at {DSN} ({exc}). Run: docker compose up -d db")
    server.reset_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def make_node(admin, tmp_path) -> Iterator[Callable[..., Node]]:
    """Factory for nodes, each with its own SQLite file and its own link."""
    nodes: list[Node] = []

    def factory(node_id: str, **kwargs) -> Node:
        node = Node(node_id, Link(DSN), str(tmp_path / f"{node_id}.sqlite3"), **kwargs)
        node.sync.listen()
        node.sync.catch_up()  # first start: full reconciliation, sets watermarks
        nodes.append(node)
        return node

    yield factory
    for node in nodes:
        node.close()


def settle(*nodes: Node) -> None:
    """Drive every node to a quiet state: upload everything, then catch up."""
    for _ in range(2):
        for node in nodes:
            node.sync.push()
        for node in nodes:
            node.sync.catch_up()
