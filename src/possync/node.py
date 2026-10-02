"""A local node: a store PC with its own SQLite database.

The node keeps working with no network. Every change is written to the local
tables and, in the same transaction, to the outbox; the sync engine uploads the
outbox later. Stock changes are recorded as deltas (-2 for a sale of two units),
never as absolute values, so sales on different nodes add up instead of
overwriting each other.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from importlib import resources

from .ids import new_id, seed_id, seed_op_id
from .server import TABLES
from .sync import SyncEngine
from .transport import Link


class Node:
    def __init__(
        self,
        node_id: str,
        link: Link,
        db_path: str = ":memory:",
        *,
        batch_size: int = 50,
        max_attempts: int = 5,
    ) -> None:
        self.node_id = node_id
        self.link = link
        self.db = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        schema = resources.files("possync").joinpath("sql/node.sql").read_text(encoding="utf-8")
        self.db.executescript(schema)
        self.sync = SyncEngine(self, link, batch_size=batch_size, max_attempts=max_attempts)

    def close(self) -> None:
        self.link.close()
        self.db.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """Local transaction. Nested calls join the outer transaction."""
        if self.db.in_transaction:
            yield self.db
            return
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.db.execute("COMMIT")

    # ── Outbox ────────────────────────────────────────────────

    def enqueue(
        self,
        kind: str,
        table: str,
        row_id: str,
        payload: dict | None = None,
        op_id: str | None = None,
    ) -> None:
        """Add an operation to the outbox. Call it inside the same transaction as
        the change it describes.

        Upserts are coalesced: if an upsert for the same row is already pending,
        nothing is added, because the payload is read from the local row at send
        time and will carry every edit made so far.
        """
        if kind == "upsert" and self._pending_exists(table, row_id, ("upsert",)):
            return
        self.db.execute(
            "INSERT OR IGNORE INTO outbox (op_id, kind, table_name, row_id, payload, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (op_id or new_id(), kind, table, row_id, json.dumps(payload or {}), _now()),
        )

    def _pending_exists(self, table: str, row_id: str, kinds: tuple[str, ...]) -> bool:
        marks = ",".join("?" * len(kinds))
        row = self.db.execute(
            f"SELECT 1 FROM outbox WHERE table_name = ? AND row_id = ? AND status = 'pending' "
            f"AND kind IN ({marks}) LIMIT 1",
            (table, row_id, *kinds),
        ).fetchone()
        return row is not None

    def pending_delta(self, product_id: str) -> int:
        """Sum of this node's stock deltas not yet confirmed by the server."""
        row = self.db.execute(
            "SELECT COALESCE(SUM(json_extract(payload, '$.delta')), 0) AS total FROM outbox "
            "WHERE kind = 'delta' AND row_id = ? AND status = 'pending'",
            (product_id,),
        ).fetchone()
        return int(row["total"])

    def row_payload(self, table: str, row_id: str) -> dict | None:
        """Current local values of the replicated columns (None if the row is gone)."""
        spec = TABLES[table]
        row = self.db.execute(
            f"SELECT {', '.join(spec.columns)} FROM {table} WHERE id = ?", (row_id,)
        ).fetchone()
        return dict(row) if row else None

    # ── Business operations (work offline) ────────────────────

    def seed_product(self, sku: str, name: str, price_cents: int, initial_stock: int) -> str:
        """Create a seed product that every node ships with.

        The id is a UUIDv5 of the SKU and the initial stock is a delta with a
        deterministic op_id, so seeding on N nodes yields one product with the
        initial stock applied exactly once.
        """
        product_id = seed_id("products", sku)
        with self.tx():
            exists = self.db.execute("SELECT 1 FROM products WHERE id = ?", (product_id,))
            if exists.fetchone():
                return product_id
            self.db.execute(
                "INSERT INTO products (id, sku, name, price_cents, stock) VALUES (?, ?, ?, ?, ?)",
                (product_id, sku, name, price_cents, initial_stock),
            )
            self.enqueue("upsert", "products", product_id)
            if initial_stock:
                self.enqueue(
                    "delta",
                    "products",
                    product_id,
                    {"delta": initial_stock},
                    op_id=seed_op_id("delta", "products", sku),
                )
        return product_id

    def create_product(self, sku: str, name: str, price_cents: int, initial_stock: int = 0) -> str:
        product_id = new_id()
        with self.tx():
            self.db.execute(
                "INSERT INTO products (id, sku, name, price_cents, stock) VALUES (?, ?, ?, ?, ?)",
                (product_id, sku, name, price_cents, initial_stock),
            )
            self.enqueue("upsert", "products", product_id)
            if initial_stock:
                self.enqueue("delta", "products", product_id, {"delta": initial_stock})
        return product_id

    def update_product(
        self, product_id: str, *, name: str | None = None, price_cents: int | None = None
    ) -> None:
        with self.tx():
            if name is not None:
                self.db.execute("UPDATE products SET name = ? WHERE id = ?", (name, product_id))
            if price_cents is not None:
                self.db.execute(
                    "UPDATE products SET price_cents = ? WHERE id = ?", (price_cents, product_id)
                )
            self.enqueue("upsert", "products", product_id)

    def delete_product(self, product_id: str) -> None:
        with self.tx():
            self.db.execute("DELETE FROM products WHERE id = ?", (product_id,))
            self.enqueue("delete", "products", product_id)

    def sell(self, product_id: str, quantity: int) -> str:
        """Record a sale. Allowed while offline; stock may go negative (an
        oversell is a business fact to surface, not something to hide)."""
        if quantity <= 0:
            raise ValueError("quantity must be positive")
        sale_id = new_id()
        with self.tx():
            product = self.db.execute(
                "SELECT price_cents FROM products WHERE id = ?", (product_id,)
            ).fetchone()
            if product is None:
                raise KeyError(f"unknown product {product_id}")
            self.db.execute(
                "INSERT INTO sales (id, product_id, quantity, unit_price_cents, node_id, sold_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (sale_id, product_id, quantity, product["price_cents"], self.node_id, _now()),
            )
            self.db.execute(
                "UPDATE products SET stock = stock - ? WHERE id = ?", (quantity, product_id)
            )
            self.enqueue("upsert", "sales", sale_id)
            self.enqueue("delta", "products", product_id, {"delta": -quantity})
        return sale_id

    def restock(self, product_id: str, quantity: int) -> None:
        with self.tx():
            self.db.execute(
                "UPDATE products SET stock = stock + ? WHERE id = ?", (quantity, product_id)
            )
            self.enqueue("delta", "products", product_id, {"delta": quantity})

    # ── Applying server state ─────────────────────────────────

    def apply_tombstone(self, table: str, row_id: str) -> bool:
        """Apply a deletion made elsewhere. Deletes win over unsent local edits:
        the server rejects those edits as 'tombstoned', so keeping the row here
        would leave this node diverged forever."""
        cur = self.db.execute(f"DELETE FROM {table} WHERE id = ?", (row_id,))
        return cur.rowcount > 0

    # ── Reads ─────────────────────────────────────────────────

    def stock(self, product_id: str) -> int | None:
        row = self.db.execute("SELECT stock FROM products WHERE id = ?", (product_id,)).fetchone()
        return None if row is None else int(row["stock"])

    def product(self, product_id: str) -> dict | None:
        row = self.db.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
        return dict(row) if row else None

    def count(self, table: str) -> int:
        return int(self.db.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])

    def outbox(self, status: str | None = None) -> list[dict]:
        if status:
            rows = self.db.execute(
                "SELECT * FROM outbox WHERE status = ? ORDER BY seq", (status,)
            ).fetchall()
        else:
            rows = self.db.execute("SELECT * FROM outbox ORDER BY seq").fetchall()
        return [dict(r) for r in rows]

    def pending_count(self) -> int:
        row = self.db.execute("SELECT COUNT(*) AS n FROM outbox WHERE status = 'pending'")
        return int(row.fetchone()["n"])

    def retry_parked(self) -> int:
        """Put parked items back in the queue (after fixing their cause)."""
        cur = self.db.execute(
            "UPDATE outbox SET status = 'pending', attempts = 0 WHERE status = 'parked'"
        )
        return cur.rowcount


def _now() -> str:
    return datetime.now(UTC).isoformat()
