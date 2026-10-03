"""Identifiers.

Every replicated row and every outbox operation is identified by a UUID, so any
node can create records while offline without coordinating with the others.

Seed records (data every node ships with, such as a starter catalog) use UUIDv5:
the same name always produces the same id, so N nodes seeding the same product
converge on one row instead of creating N copies.
"""

import uuid

# Fixed namespace for this project. Changing it would change every seed id.
NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://github.com/martinmsanchezm/pos-offline-sync")


def new_id() -> str:
    """Random id for a record created at runtime."""
    return str(uuid.uuid4())


def seed_id(table: str, natural_key: str) -> str:
    """Deterministic id for a seed record, identical on every node."""
    return str(uuid.uuid5(NAMESPACE, f"{table}:{natural_key}"))


def seed_op_id(kind: str, table: str, natural_key: str) -> str:
    """Deterministic id for a seed *operation* (for example the initial stock delta).

    Because the server deduplicates operations by id, the initial stock of a seed
    product is applied once even if every node enqueues it.
    """
    return str(uuid.uuid5(NAMESPACE, f"op:{kind}:{table}:{natural_key}"))
