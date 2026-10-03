"""End-to-end sync scenarios against a real PostgreSQL."""

from __future__ import annotations

import random
import threading

import pytest

from possync import server, sync
from possync.ids import new_id

from .conftest import settle

RICE = ("RICE-1KG", "Rice 1kg", 4_500, 100)


def server_stock(admin, product_id):
    return server.stock_of(admin, product_id)


def server_count(admin, table):
    return admin.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]


# ── Stock as deltas ───────────────────────────────────────────


def test_concurrent_sales_on_many_nodes_converge(admin, make_node):
    nodes = [make_node(f"store-{i}") for i in range(4)]
    product = None
    for node in nodes:
        product = node.seed_product(*RICE)
    settle(*nodes)

    sold = {n.node_id: 0 for n in nodes}

    def run(node, seed):
        rng = random.Random(seed)
        for _ in range(25):
            qty = rng.randint(1, 3)
            node.sell(product, qty)
            sold[node.node_id] += qty
            if rng.random() < 0.4:
                node.sync.sync_once()

    threads = [threading.Thread(target=run, args=(n, i)) for i, n in enumerate(nodes)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    settle(*nodes)

    expected = 100 - sum(sold.values())
    assert server_stock(admin, product) == expected
    assert [n.stock(product) for n in nodes] == [expected] * len(nodes)
    assert server_count(admin, "sales") == sum(n.count("sales") for n in nodes) // len(nodes)


def test_unsent_local_sale_is_not_overwritten_by_incoming_stock(admin, make_node):
    a, b = make_node("a"), make_node("b")
    product = a.seed_product(*RICE)
    settle(a, b)

    b.sell(product, 5)  # not uploaded yet
    a.sell(product, 3)
    a.sync.push()

    # Download before B uploads: B must keep its own unsent sale.
    for table in server.REPLICATED_TABLES:
        b.sync._catch_up_table(table)
    assert b.stock(product) == 100 - 3 - 5

    settle(a, b)
    assert server_stock(admin, product) == 92
    assert a.stock(product) == b.stock(product) == 92


# ── Offline node ──────────────────────────────────────────────


def test_offline_node_keeps_selling_and_converges_on_reconnect(admin, make_node):
    a, b, c = make_node("a"), make_node("b"), make_node("c")
    product = a.seed_product(*RICE)
    settle(a, b, c)

    c.sync.go_offline()
    for _ in range(10):
        c.sell(product, 1)
    assert c.sync.push().offline
    assert c.pending_count() == 20  # 10 sales + 10 deltas, all waiting

    a.sell(product, 4)
    b.sell(product, 6)
    settle(a, b)
    assert server_stock(admin, product) == 90

    c.sync.reconnect()
    settle(a, b, c)
    assert server_stock(admin, product) == 80
    assert [n.stock(product) for n in (a, b, c)] == [80, 80, 80]
    assert a.count("sales") == b.count("sales") == c.count("sales") == 12


def test_changes_missed_while_offline_arrive_through_catch_up(admin, make_node):
    a, b = make_node("a"), make_node("b")
    product = a.seed_product(*RICE)
    settle(a, b)

    b.sync.go_offline()
    a.update_product(product, price_cents=4_900)
    a.sync.push()
    assert not b.link.listening  # the notification for the price change is lost

    b.sync.reconnect()
    assert b.product(product)["price_cents"] == 4_900


def test_notifications_deliver_changes_to_online_nodes(admin, make_node):
    a, b = make_node("a"), make_node("b")
    product = a.seed_product(*RICE)
    settle(a, b)

    a.update_product(product, name="Rice 1kg (premium)")
    a.sync.push()
    applied = b.sync.process_notifications(timeout=1.0)
    assert applied >= 1
    assert b.product(product)["name"] == "Rice 1kg (premium)"


# ── Deletes and tombstones ────────────────────────────────────


def test_delete_reaches_a_node_that_was_offline(admin, make_node):
    a, b = make_node("a"), make_node("b")
    product = a.seed_product(*RICE)
    settle(a, b)

    b.sync.go_offline()
    a.delete_product(product)
    a.sync.push()
    assert b.product(product) is not None

    b.sync.reconnect()
    assert b.product(product) is None
    assert server_count(admin, "tombstones") == 1


def test_reconciliation_does_not_resurrect_a_deleted_row(admin, make_node):
    a, b = make_node("a"), make_node("b")
    product = a.seed_product(*RICE)
    settle(a, b)

    b.sync.go_offline()
    a.delete_product(product)
    a.sync.push()

    b.link.set_online(True)
    report = b.sync.reconcile()
    assert report.deleted == 1
    assert report.reuploaded == 0
    assert b.product(product) is None
    assert server_stock(admin, product) is None


def test_delete_wins_over_an_offline_edit(admin, make_node):
    a, b = make_node("a"), make_node("b")
    product = a.seed_product(*RICE)
    settle(a, b)

    b.sync.go_offline()
    b.update_product(product, price_cents=9_999)  # edit while offline
    a.delete_product(product)
    a.sync.push()

    b.sync.reconnect()
    settle(a, b)
    assert server_stock(admin, product) is None
    assert a.product(product) is None and b.product(product) is None
    results = {i["result"] for i in b.outbox("synced") if i["table_name"] == "products"}
    assert results & {"tombstoned", "obsolete"}


def test_sales_of_a_deleted_product_are_kept(admin, make_node):
    a, b = make_node("a"), make_node("b")
    product = a.seed_product(*RICE)
    a.sell(product, 2)
    settle(a, b)
    a.delete_product(product)
    settle(a, b)
    assert server_count(admin, "sales") == 1
    assert b.count("sales") == 1


# ── Retries and parking ───────────────────────────────────────


def test_data_errors_are_retried_then_parked_without_blocking_the_queue(admin, make_node):
    a = make_node("a", max_attempts=3)
    product = a.seed_product(*RICE)
    settle(a)

    ghost = new_id()  # a product the server has never seen
    with a.tx():
        a.enqueue("delta", "products", ghost, {"delta": -1})
    a.sell(product, 1)  # valid work queued behind the poisoned item

    report = a.sync.push()
    assert report.failed == 1 and report.synced == 2  # the queue was not blocked
    a.sync.push()
    a.sync.push()

    parked = a.outbox("parked")
    assert len(parked) == 1 and parked[0]["row_id"] == ghost
    assert parked[0]["attempts"] == 3
    assert "does not exist" in parked[0]["last_error"]
    assert a.pending_count() == 0
    assert server_stock(admin, product) == 99


def test_network_errors_do_not_count_as_attempts(admin, make_node):
    a = make_node("a", max_attempts=2)
    product = a.seed_product(*RICE)
    settle(a)

    a.sync.go_offline()
    a.sell(product, 1)
    for _ in range(5):
        assert a.sync.push().offline
    assert {i["attempts"] for i in a.outbox("pending")} == {0}

    a.sync.reconnect()
    assert a.pending_count() == 0
    assert server_stock(admin, product) == 99


def test_parked_items_can_be_retried(admin, make_node):
    a = make_node("a", max_attempts=1)
    a.seed_product(*RICE)
    settle(a)
    with a.tx():
        a.enqueue("delta", "products", new_id(), {"delta": 1})
    a.sync.push()
    assert len(a.outbox("parked")) == 1
    assert a.retry_parked() == 1
    assert a.pending_count() == 1


# ── Idempotency ───────────────────────────────────────────────


def test_a_lost_reply_does_not_apply_a_delta_twice(admin, make_node):
    a = make_node("a")
    product = a.seed_product(*RICE)
    settle(a)

    a.sell(product, 3)
    a.link.lose_next_reply = True
    assert a.sync.push().offline  # the server committed, but A never heard back
    assert server_stock(admin, product) == 97
    assert a.pending_count() == 2

    report = a.sync.push()  # retry the same operations
    assert report.statuses.get("duplicate") == 1
    assert server_stock(admin, product) == 97
    assert server_count(admin, "sales") == 1


def test_retried_upsert_does_not_touch_the_row_again(admin, make_node):
    a = make_node("a")
    product = a.create_product("BEANS-1KG", "Beans 1kg", 6_200)
    a.link.lose_next_reply = True
    a.sync.push()
    first = admin.execute("SELECT updated_at FROM products WHERE id = %s", (product,)).fetchone()

    report = a.sync.push()
    assert report.statuses == {"unchanged": 1}
    again = admin.execute("SELECT updated_at FROM products WHERE id = %s", (product,)).fetchone()
    assert again == first
    assert server_count(admin, "products") == 1


def test_seed_products_converge_to_a_single_row(admin, make_node):
    nodes = [make_node(f"store-{i}") for i in range(3)]
    ids = {n.seed_product(*RICE) for n in nodes}
    settle(*nodes)

    assert len(ids) == 1
    product = ids.pop()
    assert server_count(admin, "products") == 1
    assert server_stock(admin, product) == 100  # the seed stock was applied once, not 3 times
    assert [n.stock(product) for n in nodes] == [100, 100, 100]


# ── Reconciliation ────────────────────────────────────────────


def test_reconciliation_reuploads_a_row_whose_upload_was_lost(admin, make_node):
    a = make_node("a")
    product = a.create_product("OIL-1L", "Oil 1L", 12_000)
    # Simulate an upload that never happened (for example a cleared parked item).
    a.db.execute("UPDATE outbox SET status = 'synced'")
    assert server_count(admin, "products") == 0

    report = a.sync.reconcile()
    assert report.reuploaded == 1
    assert server_count(admin, "products") == 1
    assert server.fetch_rows(admin, "products", [product])[0]["name"] == "Oil 1L"


def test_reconciliation_restores_rows_missing_locally(admin, make_node):
    a, b = make_node("a"), make_node("b")
    product = a.seed_product(*RICE)
    a.sell(product, 1)
    settle(a, b)

    b.db.execute("DELETE FROM sales")  # local damage
    b.db.execute("UPDATE products SET stock = 0, name = 'garbage'")
    report = b.sync.reconcile()
    assert report.downloaded >= 2
    assert b.count("sales") == 1
    assert b.product(product)["name"] == "Rice 1kg"
    assert b.stock(product) == 99


def test_catch_up_pages_through_rows_sharing_one_timestamp(admin, make_node, monkeypatch):
    a, b = make_node("a"), make_node("b")
    b.sync.go_offline()
    for i in range(10):
        a.create_product(f"SKU-{i}", f"Product {i}", 1_000)
    a.sync.push()

    # Force all ten rows onto the exact same server timestamp.
    admin.execute("ALTER TABLE products DISABLE TRIGGER trg_products_touch")
    admin.execute("UPDATE products SET updated_at = clock_timestamp()")
    admin.execute("ALTER TABLE products ENABLE TRIGGER trg_products_touch")

    monkeypatch.setattr(sync, "CATCHUP_PAGE", 3)
    b.sync.reconnect()
    assert b.count("products") == 10


def test_watermarks_come_from_server_time(admin, make_node):
    a = make_node("a")
    a.create_product("SALT", "Salt", 1_500)
    a.sync.push()
    a.sync.catch_up()
    watermark = a.sync._watermark("products")
    newest = admin.execute("SELECT max(updated_at) AS m FROM products").fetchone()["m"]
    assert watermark >= newest
    assert watermark <= server.server_now(admin)


@pytest.mark.parametrize("bad_qty", [0, -2])
def test_invalid_sales_are_rejected_locally(make_node, bad_qty):
    a = make_node("a")
    product = a.seed_product(*RICE)
    with pytest.raises(ValueError):
        a.sell(product, bad_qty)
