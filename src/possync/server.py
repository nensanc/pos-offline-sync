"""Central server: the operations nodes call on PostgreSQL.

Every function takes an open psycopg connection. Nodes never call these
directly; they go through `transport.Link`, which simulates the network.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources

import psycopg
from psycopg import sql


@dataclass(frozen=True)
class TableSpec:
    """Columns a node replicates for a table. `stock` is never one of them."""

    name: str
    columns: tuple[str, ...]


# Dependency order: parents before children.
TABLES: dict[str, TableSpec] = {
    "products": TableSpec("products", ("id", "sku", "name", "price_cents")),
    "sales": TableSpec(
        "sales", ("id", "product_id", "quantity", "unit_price_cents", "node_id", "sold_at")
    ),
}
REPLICATED_TABLES = tuple(TABLES)


# ── Schema ────────────────────────────────────────────────────


def init_schema(conn: psycopg.Connection) -> None:
    ddl = resources.files("possync").joinpath("sql/server.sql").read_text(encoding="utf-8")
    with conn.transaction():
        conn.execute(ddl)


def reset_schema(conn: psycopg.Connection) -> None:
    """Drop everything (tests and the demo start from a clean server)."""
    with conn.transaction():
        conn.execute("DROP TABLE IF EXISTS products, sales CASCADE")
    init_schema(conn)


# ── Writes ────────────────────────────────────────────────────


def apply_batch(conn: psycopg.Connection, node_id: str, ops: list[dict]) -> list[dict]:
    """Apply a batch of outbox operations in one transaction.

    Each operation runs in its own savepoint, so one invalid item fails alone and
    the rest of the batch still commits. Returns one result per operation:
    {"ok": True, "status": ...} or {"ok": False, "error": ...}.
    """
    results: list[dict] = []
    with conn.transaction():
        for op in ops:
            try:
                with conn.transaction():
                    status = _apply_one(conn, node_id, op)
                results.append({"ok": True, "status": status})
            except psycopg.OperationalError:
                raise  # the connection itself failed: a network problem, not a data one
            except psycopg.Error as exc:
                results.append({"ok": False, "error": _error_text(exc)})
    return results


def _apply_one(conn: psycopg.Connection, node_id: str, op: dict) -> str:
    kind = op["kind"]
    spec = TABLES[op["table"]]
    if kind == "upsert":
        return _upsert(conn, spec, op["payload"], node_id)
    if kind == "delete":
        conn.execute(
            sql.SQL("DELETE FROM {} WHERE id = %s").format(sql.Identifier(spec.name)),
            (op["row_id"],),
        )
        return "applied"
    raise ValueError(f"unknown operation kind: {kind}")


def _upsert(conn: psycopg.Connection, spec: TableSpec, payload: dict, node_id: str) -> str:
    """Idempotent upsert by id. Only the replicated columns are written: the
    initial stock of a new product arrives as a separate delta.

    The `IS DISTINCT FROM` guard skips no-op updates, so retrying an operation
    does not bump `updated_at` or send another notification.
    """
    cols = list(spec.columns)
    data_cols = [c for c in cols if c != "id"]
    query = sql.SQL(
        "INSERT INTO {table} ({cols}, origin) VALUES ({vals}, %s) "
        "ON CONFLICT (id) DO UPDATE SET {sets}, origin = EXCLUDED.origin "
        "WHERE ({current}) IS DISTINCT FROM ({incoming}) "
        "RETURNING id"
    ).format(
        table=sql.Identifier(spec.name),
        cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
        vals=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
        sets=sql.SQL(", ").join(
            sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in data_cols
        ),
        current=sql.SQL(", ").join(
            sql.SQL("{t}.{c}").format(t=sql.Identifier(spec.name), c=sql.Identifier(c))
            for c in data_cols
        ),
        incoming=sql.SQL(", ").join(
            sql.SQL("EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in data_cols
        ),
    )
    row = conn.execute(query, [payload[c] for c in cols] + [node_id]).fetchone()
    return "applied" if row else "unchanged"


def _error_text(exc: psycopg.Error) -> str:
    diag = getattr(exc, "diag", None)
    primary = diag.message_primary if diag and diag.message_primary else str(exc)
    return f"{type(exc).__name__}: {primary}"[:500]


# ── Introspection (demo and tests) ────────────────────────────


def stock_of(conn: psycopg.Connection, product_id: str) -> int | None:
    row = conn.execute("SELECT stock FROM products WHERE id = %s", (product_id,)).fetchone()
    return None if row is None else int(row["stock"])
