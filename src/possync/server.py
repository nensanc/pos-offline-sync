"""Central server: the operations nodes call on PostgreSQL.

Every function takes an open psycopg connection. Nodes never call these
directly; they go through `transport.Link`, which simulates the network.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from importlib import resources
from typing import Any

import psycopg
from psycopg import sql

NIL_UUID = "00000000-0000-0000-0000-000000000000"


@dataclass(frozen=True)
class TableSpec:
    """Columns a node replicates for a table. `stock` is never one of them:
    it only moves through stock deltas."""

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
        conn.execute("DROP TABLE IF EXISTS products, sales, tombstones, applied_ops CASCADE")
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
    if kind == "delta":
        row = conn.execute(
            "SELECT apply_stock_delta(%s::uuid, %s::uuid, %s::bigint, %s::text) AS status",
            (op["op_id"], op["row_id"], op["payload"]["delta"], node_id),
        ).fetchone()
        return row["status"]

    spec = TABLES[op["table"]]
    if kind == "upsert":
        if _is_tombstoned(conn, spec.name, op["row_id"]):
            return "tombstoned"  # deletes win: a stale edit cannot resurrect a row
        return _upsert(conn, spec, op["payload"], node_id)
    if kind == "delete":
        conn.execute(
            sql.SQL("DELETE FROM {} WHERE id = %s").format(sql.Identifier(spec.name)),
            (op["row_id"],),
        )
        conn.execute(
            "INSERT INTO tombstones (table_name, row_id, origin) VALUES (%s, %s, %s) "
            "ON CONFLICT (table_name, row_id) DO NOTHING",
            (spec.name, op["row_id"], node_id),
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


def _is_tombstoned(conn: psycopg.Connection, table: str, row_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM tombstones WHERE table_name = %s AND row_id = %s", (table, row_id)
    ).fetchone()
    return row is not None


def _error_text(exc: psycopg.Error) -> str:
    diag = getattr(exc, "diag", None)
    primary = diag.message_primary if diag and diag.message_primary else str(exc)
    return f"{type(exc).__name__}: {primary}"[:500]


# ── Reads (the three download layers) ─────────────────────────


def server_now(conn: psycopg.Connection) -> datetime:
    return conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]


def fetch_rows(conn: psycopg.Connection, table: str, ids: list[str]) -> list[dict]:
    """Layer 1 (notifications): read the rows a notification pointed at."""
    spec = TABLES[table]
    rows = conn.execute(
        sql.SQL("SELECT * FROM {} WHERE id = ANY(%s::uuid[])").format(sql.Identifier(spec.name)),
        (ids,),
    ).fetchall()
    return [_clean(r) for r in rows]


def fetch_tombstones(conn: psycopg.Connection, table: str, ids: list[str]) -> list[str]:
    rows = conn.execute(
        "SELECT row_id FROM tombstones WHERE table_name = %s AND row_id = ANY(%s::uuid[])",
        (table, ids),
    ).fetchall()
    return [str(r["row_id"]) for r in rows]


def changes_since(
    conn: psycopg.Connection, table: str, after_ts: datetime, after_id: str, limit: int
) -> list[dict]:
    """Layer 2 (catch-up): rows changed after a (timestamp, id) cursor.

    Keyset pagination on (updated_at, id) cannot get stuck when many rows share
    the same timestamp, which a plain `updated_at > x` cursor would.
    """
    spec = TABLES[table]
    rows = conn.execute(
        sql.SQL(
            "SELECT * FROM {} WHERE (updated_at, id) > (%s, %s) ORDER BY updated_at, id LIMIT %s"
        ).format(sql.Identifier(spec.name)),
        (after_ts, after_id, limit),
    ).fetchall()
    return [_clean(r) for r in rows]


def tombstones_since(
    conn: psycopg.Connection, after_ts: datetime, after_id: str, limit: int
) -> list[dict]:
    rows = conn.execute(
        "SELECT table_name, row_id, deleted_at FROM tombstones "
        "WHERE (deleted_at, row_id) > (%s, %s) ORDER BY deleted_at, row_id LIMIT %s",
        (after_ts, after_id, limit),
    ).fetchall()
    return [_clean(r) for r in rows]


def snapshot(conn: psycopg.Connection) -> dict[str, Any]:
    """Layer 3 (reconciliation): a consistent copy of every replicated table and
    all tombstones, plus the server time at which it was taken."""
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        taken_at = server_now(conn)
        tables = {
            t: [
                _clean(r)
                for r in conn.execute(
                    sql.SQL("SELECT * FROM {}").format(sql.Identifier(t))
                ).fetchall()
            ]
            for t in REPLICATED_TABLES
        }
        tombstones = [
            _clean(r)
            for r in conn.execute(
                "SELECT table_name, row_id, deleted_at FROM tombstones"
            ).fetchall()
        ]
    return {"taken_at": taken_at, "tables": tables, "tombstones": tombstones}


def _clean(row: dict) -> dict:
    """UUIDs as strings, so rows compare equal to what the nodes store."""
    return {k: (str(v) if isinstance(v, uuid.UUID) else v) for k, v in row.items()}


# ── Introspection (demo and tests) ────────────────────────────


def stock_of(conn: psycopg.Connection, product_id: str) -> int | None:
    row = conn.execute("SELECT stock FROM products WHERE id = %s", (product_id,)).fetchone()
    return None if row is None else int(row["stock"])
