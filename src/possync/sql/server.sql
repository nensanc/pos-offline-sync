-- Central PostgreSQL schema.
--
-- Rules this schema enforces:
--   * `updated_at` is always set by the server (clock_timestamp()), never by a
--     client. Nodes use it as their catch-up watermark, so client clock skew can
--     never make a node skip changes.
--   * `products.stock` only changes through apply_stock_delta(). Upserts never
--     write it, which is what lets concurrent sales on different nodes add up.
--   * Every write emits a notification on the `possync_changes` channel. It is a
--     hint, not a guarantee: a node that is offline simply misses it.

CREATE TABLE IF NOT EXISTS products (
    id          uuid PRIMARY KEY,
    sku         text        NOT NULL,
    name        text        NOT NULL,
    price_cents integer     NOT NULL DEFAULT 0 CHECK (price_cents >= 0),
    -- Integer units (grams for products sold by weight) to keep sums exact.
    stock       bigint      NOT NULL DEFAULT 0,
    origin      text,
    updated_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Sales are immutable facts. No foreign key on purpose: the sales history must
-- survive the deletion of a product.
CREATE TABLE IF NOT EXISTS sales (
    id               uuid PRIMARY KEY,
    product_id       uuid        NOT NULL,
    quantity         bigint      NOT NULL CHECK (quantity > 0),
    unit_price_cents integer     NOT NULL CHECK (unit_price_cents >= 0),
    node_id          text        NOT NULL,
    sold_at          text        NOT NULL,
    origin           text,
    updated_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Tombstones: durable records of deletions. A node that was offline when a row
-- was deleted learns about it here, and reconciliation uses them so a deleted
-- row is never resurrected by a node that still has a copy.
CREATE TABLE IF NOT EXISTS tombstones (
    table_name text        NOT NULL,
    row_id     uuid        NOT NULL,
    origin     text,
    deleted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (table_name, row_id)
);

-- Ids of stock deltas already applied. Makes apply_stock_delta() idempotent:
-- a retried delta (for example after a lost acknowledgement) is not applied twice.
CREATE TABLE IF NOT EXISTS applied_ops (
    op_id      uuid PRIMARY KEY,
    node_id    text        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS ix_products_updated   ON products   (updated_at, id);
CREATE INDEX IF NOT EXISTS ix_sales_updated      ON sales      (updated_at, id);
CREATE INDEX IF NOT EXISTS ix_tombstones_deleted ON tombstones (deleted_at, row_id);

-- Server-assigned modification time.
CREATE OR REPLACE FUNCTION possync_touch() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END $$;

-- Change notification (best effort).
CREATE OR REPLACE FUNCTION possync_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_id uuid;
BEGIN
    IF TG_TABLE_NAME = 'tombstones' THEN
        v_id := NEW.row_id;
        PERFORM pg_notify('possync_changes',
            json_build_object('table', NEW.table_name, 'id', v_id, 'op', 'DELETE')::text);
    ELSE
        v_id := COALESCE(NEW.id, OLD.id);
        PERFORM pg_notify('possync_changes',
            json_build_object('table', TG_TABLE_NAME, 'id', v_id, 'op', TG_OP)::text);
    END IF;
    RETURN NULL;
END $$;

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['products', 'sales'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%1$s_touch ON %1$s', t);
        EXECUTE format('CREATE TRIGGER trg_%1$s_touch BEFORE INSERT OR UPDATE ON %1$s
                        FOR EACH ROW EXECUTE FUNCTION possync_touch()', t);
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%1$s_notify ON %1$s', t);
        EXECUTE format('CREATE TRIGGER trg_%1$s_notify AFTER INSERT OR UPDATE ON %1$s
                        FOR EACH ROW EXECUTE FUNCTION possync_notify()', t);
    END LOOP;
    DROP TRIGGER IF EXISTS trg_tombstones_notify ON tombstones;
    CREATE TRIGGER trg_tombstones_notify AFTER INSERT ON tombstones
        FOR EACH ROW EXECUTE FUNCTION possync_notify();
END $$;

-- Apply a stock delta exactly once.
--   'applied'    the delta was added to the stock
--   'duplicate'  this op_id was already applied (retry after a lost ack)
--   'tombstoned' the product was deleted; the delta has nothing to apply to
-- Raises if the product does not exist yet, so the node retries later (its
-- create may still be in flight) and eventually parks the item.
CREATE OR REPLACE FUNCTION apply_stock_delta(
    p_op_id      uuid,
    p_product_id uuid,
    p_delta      bigint,
    p_node_id    text
) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO applied_ops (op_id, node_id) VALUES (p_op_id, p_node_id)
    ON CONFLICT (op_id) DO NOTHING;
    IF NOT FOUND THEN
        RETURN 'duplicate';
    END IF;

    UPDATE products SET stock = stock + p_delta WHERE id = p_product_id;
    IF FOUND THEN
        RETURN 'applied';
    END IF;

    IF EXISTS (SELECT 1 FROM tombstones
               WHERE table_name = 'products' AND row_id = p_product_id) THEN
        RETURN 'tombstoned';
    END IF;

    -- Undo the applied_ops insert (the whole call runs in the caller's savepoint).
    RAISE EXCEPTION 'product % does not exist on the server', p_product_id
        USING ERRCODE = 'foreign_key_violation';
END $$;
