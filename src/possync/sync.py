"""Sync engine: upload the outbox to the server.

The outbox is sent in batches, oldest first. Each item is retried until the
server accepts it. A *data* error (the server answered, but rejected the
item) counts as an attempt; after `max_attempts` the item is parked so it
stops blocking the queue. A *network* error never counts: the item is fine,
the network is not.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from . import server
from .errors import NetworkError

if TYPE_CHECKING:
    from .node import Node
    from .transport import Link


@dataclass
class PushReport:
    sent: int = 0
    synced: int = 0
    failed: int = 0
    parked: int = 0
    offline: bool = False
    statuses: dict[str, int] = field(default_factory=dict)


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

    # ── Connectivity ──────────────────────────────────────────

    def go_offline(self) -> None:
        self.link.set_online(False)
