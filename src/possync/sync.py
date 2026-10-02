"""Sync engine: upload the outbox and download changes in three layers.

Upload (push)
    The outbox is sent in batches, oldest first. Each item is retried until the
    server accepts it. A *data* error (the server answered, but rejected the
    item) counts as an attempt; after `max_attempts` the item is parked so it
    stops blocking the queue. A *network* error never counts: the item is fine,
    the network is not.

Download (pull), three layers from cheapest to most thorough
    1. Notifications: the server pushes "table X, row Y changed". Fast, but best
       effort: anything sent while the node was offline is lost.
    2. Catch-up: per table, read everything changed since a watermark. The
       watermark is always a server timestamp, so client clock skew is
       irrelevant. Run on reconnect and periodically.
    3. Reconciliation: compare full server state with local state. Heals
       anything the other layers missed. Run on first start and on demand.

Every download step pushes first: uploading local changes before reading
remote ones means the server already includes them.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from . import server
from .errors import NetworkError
from .server import NIL_UUID, REPLICATED_TABLES

if TYPE_CHECKING:
    from .node import Node
    from .transport import Link

# Re-read this far behind the watermark. A transaction that started before the
# last catch-up but committed after it carries an older updated_at; the overlap
# picks it up. Re-applying a row is harmless because applying is idempotent.
CATCHUP_OVERLAP = timedelta(seconds=2)
CATCHUP_PAGE = 500
TOMBSTONES = "tombstones"


@dataclass
class PushReport:
    sent: int = 0
    synced: int = 0
    failed: int = 0
    parked: int = 0
    offline: bool = False
    statuses: dict[str, int] = field(default_factory=dict)


@dataclass
class ReconcileReport:
    downloaded: int = 0
    deleted: int = 0
    reuploaded: int = 0


class SyncEngine:
    def __init__(self, node: Node, link: Link, *, batch_size: int, max_attempts: int) -> None:
        self.node = node
        self.link = link
        self.batch_size = batch_size
        self.max_attempts = max_attempts

    # ── Upload ────────────────────────────────────────────────

    def push(self) -> PushReport:
        """Make one pass over the pending outbox, oldest first, in batches.

        A failed item is not retried within the same pass: it waits for the next
        cycle, so one bad item costs one attempt per cycle and never stalls the
        items queued behind it.
        """
        report = PushReport()
        last_seq = 0
        while True:
            items = self.node.db.execute(
                "SELECT * FROM outbox WHERE status = 'pending' AND seq > ? ORDER BY seq LIMIT ?",
                (last_seq, self.batch_size),
            ).fetchall()
            if not items:
                return report
            last_seq = items[-1]["seq"]

            sendable, ops = [], []
            with self.node.tx():
                for item in items:
                    op = self._to_operation(item)
                    if op is None:
                        # Upsert of a row deleted locally since: its delete follows.
                        self._mark(item, "synced", result="obsolete")
                        continue
                    sendable.append(item)
                    ops.append(op)
            if not ops:
                continue

            report.sent += len(ops)
            try:
                results = self.link.call(server.apply_batch, self.node.node_id, ops)
            except NetworkError:
                report.offline = True
                return report

            with self.node.tx():
                for item, res in zip(sendable, results, strict=True):
                    if res["ok"]:
                        self._mark(item, "synced", result=res["status"])
                        report.synced += 1
                        report.statuses[res["status"]] = report.statuses.get(res["status"], 0) + 1
                    else:
                        attempts = item["attempts"] + 1
                        status = "parked" if attempts >= self.max_attempts else "pending"
                        self.node.db.execute(
                            "UPDATE outbox SET attempts = ?, last_error = ?, status = ? "
                            "WHERE seq = ?",
                            (attempts, res["error"], status, item["seq"]),
                        )
                        report.failed += 1
                        if status == "parked":
                            report.parked += 1

    def _to_operation(self, item) -> dict | None:
        op = {
            "op_id": item["op_id"],
            "kind": item["kind"],
            "table": item["table_name"],
            "row_id": item["row_id"],
            "payload": json.loads(item["payload"]),
        }
        if item["kind"] == "upsert":
            # Hydrate at send time: the full current row, so partial edits and
            # several edits to the same row travel as one complete payload.
            payload = self.node.row_payload(item["table_name"], item["row_id"])
            if payload is None:
                return None
            op["payload"] = payload
        return op

    def _mark(self, item, status: str, *, result: str | None = None) -> None:
        self.node.db.execute(
            "UPDATE outbox SET status = ?, result = ? WHERE seq = ?", (status, result, item["seq"])
        )

    # ── Download layer 1: notifications ───────────────────────

    def listen(self) -> None:
        self.link.listen()

    def process_notifications(self, timeout: float = 0.05) -> int:
        """Apply the rows that recent notifications pointed at."""
        notes = self.link.drain_notifications(timeout)
        if not notes:
            return 0
        changed: dict[str, set[str]] = defaultdict(set)
        for note in notes:
            changed[note["table"]].add(note["id"])
        applied = 0
        try:
            for table in REPLICATED_TABLES:
                ids = sorted(changed.get(table, ()))
                if not ids:
                    continue
                rows = self.link.call(server.fetch_rows, table, ids)
                missing = sorted(set(ids) - {r["id"] for r in rows})
                gone = self.link.call(server.fetch_tombstones, table, missing) if missing else []
                with self.node.tx():
                    for row in rows:
                        self.node.apply_remote_row(table, row)
                    for row_id in gone:
                        self.node.apply_tombstone(table, row_id)
                applied += len(rows) + len(gone)
        except NetworkError:
            pass  # whatever was missed is picked up by the next catch-up
        return applied

    # ── Download layer 2: incremental catch-up ────────────────

    def catch_up(self) -> int:
        """Download everything changed since each table's watermark.

        Falls back to a full reconciliation on a node that never synced (it has
        no watermark yet).
        """
        self.push()
        if any(self._watermark(t) is None for t in (*REPLICATED_TABLES, TOMBSTONES)):
            report = self.reconcile()
            return report.downloaded + report.deleted
        applied = 0
        try:
            for table in REPLICATED_TABLES:
                applied += self._catch_up_table(table)
            applied += self._catch_up_tombstones()
        except NetworkError:
            pass
        return applied

    def _catch_up_table(self, table: str) -> int:
        watermark = self._watermark(table)
        cursor_ts, cursor_id = watermark - CATCHUP_OVERLAP, NIL_UUID
        applied = 0
        while True:
            rows = self.link.call(server.changes_since, table, cursor_ts, cursor_id, CATCHUP_PAGE)
            if not rows:
                break
            with self.node.tx():
                for row in rows:
                    self.node.apply_remote_row(table, row)
                last = rows[-1]
                cursor_ts, cursor_id = last["updated_at"], last["id"]
                if cursor_ts > watermark:
                    watermark = cursor_ts
                    self._set_watermark(table, watermark)
            applied += len(rows)
            if len(rows) < CATCHUP_PAGE:
                break
        return applied

    def _catch_up_tombstones(self) -> int:
        watermark = self._watermark(TOMBSTONES)
        cursor_ts, cursor_id = watermark - CATCHUP_OVERLAP, NIL_UUID
        deleted = 0
        while True:
            rows = self.link.call(server.tombstones_since, cursor_ts, cursor_id, CATCHUP_PAGE)
            if not rows:
                break
            with self.node.tx():
                for row in rows:
                    if self.node.apply_tombstone(row["table_name"], row["row_id"]):
                        deleted += 1
                last = rows[-1]
                cursor_ts, cursor_id = last["deleted_at"], last["row_id"]
                if cursor_ts > watermark:
                    watermark = cursor_ts
                    self._set_watermark(TOMBSTONES, watermark)
            if len(rows) < CATCHUP_PAGE:
                break
        return deleted

    # ── Download layer 3: full reconciliation ─────────────────

    def reconcile(self) -> ReconcileReport:
        """Make local state match the server, without losing local work.

        * Server rows missing or outdated locally are applied (same rules as the
          other layers: unsent local edits and deltas are preserved).
        * Tombstoned rows are deleted locally and never re-uploaded.
        * Local rows the server has never seen, with nothing pending, are
          enqueued again: their upload was lost (for example a parked item that
          was cleared), and reconciliation re-sends them.
        """
        report = ReconcileReport()
        self.push()
        snap = self.link.call(server.snapshot)
        tombstoned: dict[str, set[str]] = defaultdict(set)
        for t in snap["tombstones"]:
            tombstoned[t["table_name"]].add(t["row_id"])

        with self.node.tx():
            for table in REPLICATED_TABLES:
                for row_id in tombstoned[table]:
                    if self.node.apply_tombstone(table, row_id):
                        report.deleted += 1
                remote_ids = set()
                for row in snap["tables"][table]:
                    remote_ids.add(row["id"])
                    if self.node.apply_remote_row(table, row) in ("inserted", "updated"):
                        report.downloaded += 1
                local_ids = {
                    r["id"] for r in self.node.db.execute(f"SELECT id FROM {table}").fetchall()
                }
                for row_id in sorted(local_ids - remote_ids - tombstoned[table]):
                    if not self.node._pending_exists(table, row_id, ("upsert", "delete")):
                        self.node.enqueue("upsert", table, row_id)
                        report.reuploaded += 1

            # Everything up to the snapshot is now applied. The catch-up overlap
            # covers transactions that were still in flight when it was taken.
            for key in (*REPLICATED_TABLES, TOMBSTONES):
                self._set_watermark(key, snap["taken_at"])

        if report.reuploaded:
            self.push()
        return report

    # ── Connectivity ──────────────────────────────────────────

    def go_offline(self) -> None:
        self.link.set_online(False)

    def reconnect(self) -> int:
        """Come back online: subscribe first, then catch up. In that order,
        nothing can fall in the gap between the two."""
        self.link.set_online(True)
        self.listen()
        return self.catch_up()

    def sync_once(self) -> None:
        """One routine cycle: upload, then apply notifications."""
        self.push()
        self.process_notifications()

    # ── Watermarks ────────────────────────────────────────────

    def _watermark(self, key: str) -> datetime | None:
        value = self.node.get_state(f"watermark:{key}")
        return datetime.fromisoformat(value) if value else None

    def _set_watermark(self, key: str, value: datetime) -> None:
        self.node.set_state(f"watermark:{key}", value.isoformat())
