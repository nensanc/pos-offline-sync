"""Demo: three stores sell the same product while one of them goes offline.

    docker compose run --rm demo          # everything in containers
    python -m possync.demo                # against `docker compose up -d db`

The run ends by checking that the server and every store agree on the stock,
and that the stock equals the initial stock minus everything sold.
"""

from __future__ import annotations

import os
import random
import sys
import tempfile
import threading
import time
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from . import server
from .node import Node
from .transport import Link

DSN = os.environ.get("DATABASE_URL", "postgresql://possync:possync@localhost:5433/possync")

CATALOG = [
    ("RICE-1KG", "Rice 1kg", 4_500, 500),
    ("BEANS-1KG", "Beans 1kg", 6_200, 300),
    ("COFFEE-500G", "Coffee 500g", 15_900, 120),
]
ROUNDS = 40
OFFLINE_FROM, OFFLINE_UNTIL = 8, 28  # rounds during which "south" is offline

_print_lock = threading.Lock()
_t0 = time.monotonic()


def log(store: str, message: str) -> None:
    with _print_lock:
        print(f"  {time.monotonic() - _t0:5.2f}s  {store:<7} {message}", flush=True)


def store_loop(node: Node, products: dict[str, str], sold: dict, seed: int) -> None:
    rng = random.Random(seed)
    name = node.node_id
    for round_no in range(ROUNDS):
        if name == "south" and round_no == OFFLINE_FROM:
            node.sync.go_offline()
            log(name, "network down: keeps selling offline")
        if name == "north" and round_no == 12:
            node.delete_product(products["COFFEE-500G"])
            log(name, "discontinued Coffee 500g (delete)")
        if name == "center" and round_no == 15:
            node.update_product(products["BEANS-1KG"], price_cents=6_500)
            log(name, "raised the price of Beans 1kg")
        if name == "center" and round_no == 20:
            node.link.lose_next_reply = True
            log(name, "next server reply will be lost (fault injection)")
        if name == "south" and round_no == OFFLINE_UNTIL:
            pending = node.pending_count()
            applied = node.sync.reconnect()
            log(name, f"back online: uploaded {pending} queued ops, caught up {applied} changes")

        qty = rng.randint(1, 4)
        node.sell(products["RICE-1KG"], qty)
        sold[name] += qty
        if rng.random() < 0.3 and node.product(products["BEANS-1KG"]):
            node.sell(products["BEANS-1KG"], 1)
            sold[f"{name}:beans"] += 1

        if node.link.online and rng.random() < 0.5:
            report = node.sync.push()
            if report.statuses.get("duplicate"):
                log(
                    name,
                    f"retry after lost reply: {report.statuses['duplicate']} delta(s) "
                    "recognized as duplicates, not applied twice",
                )
            node.sync.process_notifications(timeout=0.01)
        time.sleep(rng.uniform(0.005, 0.03))


def main() -> int:
    try:
        admin = psycopg.connect(DSN, autocommit=True, row_factory=dict_row, connect_timeout=5)
    except psycopg.OperationalError as exc:
        print(f"Cannot reach PostgreSQL at {DSN}: {exc}\nStart it with: docker compose up -d db")
        return 2
    server.reset_schema(admin)

    workdir = Path(tempfile.mkdtemp(prefix="possync-demo-"))
    stores = ["north", "center", "south"]
    nodes = [Node(s, Link(DSN), str(workdir / f"{s}.sqlite3")) for s in stores]

    print("\n== Setup: every store seeds the same catalog (UUIDv5 ids, seed stock as deltas)")
    products: dict[str, str] = {}
    for node in nodes:
        for sku, name, price, stock in CATALOG:
            products[sku] = node.seed_product(sku, name, price, stock)
        node.sync.listen()
        node.sync.catch_up()
    for node in nodes:
        node.sync.catch_up()
    rice = products["RICE-1KG"]
    print(
        f"   Rice 1kg on the server: {server.stock_of(admin, rice)} units "
        f"(seeded by {len(nodes)} stores, applied once)"
    )

    print(f"\n== Selling: {ROUNDS} rounds per store, in parallel")
    sold = {s: 0 for s in stores} | {f"{s}:beans": 0 for s in stores}
    threads = [
        threading.Thread(target=store_loop, args=(n, products, sold, i))
        for i, n in enumerate(nodes)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print("\n== Final sync: every store uploads and catches up")
    for _ in range(2):
        for node in nodes:
            node.sync.push()
        for node in nodes:
            node.sync.catch_up()

    initial = dict((sku, stock) for sku, _, _, stock in CATALOG)
    rice_sold = sum(sold[s] for s in stores)
    expected_rice = initial["RICE-1KG"] - rice_sold
    server_rice = server.stock_of(admin, rice)

    print("\n== Result")
    header = f"   {'':<10}{'Rice stock':>12}{'sales rows':>12}{'pending':>9}{'parked':>8}"
    print(header)
    print("   " + "-" * (len(header) - 3))
    print(
        f"   {'server':<10}{server_rice:>12}"
        f"{admin.execute('SELECT COUNT(*) n FROM sales').fetchone()['n']:>12}"
    )
    for node in nodes:
        print(
            f"   {node.node_id:<10}{node.stock(rice):>12}{node.count('sales'):>12}"
            f"{node.pending_count():>9}{len(node.outbox('parked')):>8}"
        )

    print(f"\n   Rice sold: {', '.join(f'{s}={sold[s]}' for s in stores)} → total {rice_sold}")
    print(f"   Expected Rice stock: {initial['RICE-1KG']} - {rice_sold} = {expected_rice}")

    coffee_gone = all(n.product(products["COFFEE-500G"]) is None for n in nodes)
    beans_prices = {n.product(products["BEANS-1KG"])["price_cents"] for n in nodes}
    checks = {
        "server stock equals initial stock minus all sales": server_rice == expected_rice,
        "every store has the server's stock": all(n.stock(rice) == server_rice for n in nodes),
        "every store has every sale": len({n.count("sales") for n in nodes}) == 1,
        "the delete reached the store that was offline (tombstone)": coffee_gone,
        "the price change reached every store": beans_prices == {6_500},
        "nothing left in any outbox": all(n.pending_count() == 0 for n in nodes),
    }
    print()
    for label, ok in checks.items():
        print(f"   [{'OK' if ok else 'FAIL'}] {label}")

    for node in nodes:
        node.close()
    admin.close()
    converged = all(checks.values())
    print("\n" + ("Converged." if converged else "Did NOT converge.") + "\n")
    return 0 if converged else 1


if __name__ == "__main__":
    sys.exit(main())
