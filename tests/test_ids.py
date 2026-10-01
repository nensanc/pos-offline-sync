import uuid

from possync.ids import new_id, seed_id, seed_op_id


def test_seed_ids_are_deterministic_and_scoped():
    assert seed_id("products", "RICE-1KG") == seed_id("products", "RICE-1KG")
    assert seed_id("products", "RICE-1KG") != seed_id("products", "BEANS-1KG")
    assert seed_id("products", "X") != seed_id("sales", "X")
    assert uuid.UUID(seed_id("products", "X")).version == 5


def test_seed_operation_ids_differ_from_row_ids():
    assert seed_op_id("delta", "products", "X") != seed_id("products", "X")
    assert seed_op_id("delta", "products", "X") == seed_op_id("delta", "products", "X")


def test_runtime_ids_are_random_v4():
    ids = {new_id() for _ in range(1000)}
    assert len(ids) == 1000
    assert all(uuid.UUID(i).version == 4 for i in ids)
