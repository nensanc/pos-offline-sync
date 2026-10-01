-- Central PostgreSQL schema.
--
-- Rules this schema enforces:
--   * `updated_at` is always set by the server (clock_timestamp()), never by a
--     client.
--   * Upserts never write `products.stock`.

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

CREATE INDEX IF NOT EXISTS ix_products_updated   ON products   (updated_at, id);
CREATE INDEX IF NOT EXISTS ix_sales_updated      ON sales      (updated_at, id);

-- Server-assigned modification time.
CREATE OR REPLACE FUNCTION possync_touch() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END $$;

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['products', 'sales'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%1$s_touch ON %1$s', t);
        EXECUTE format('CREATE TRIGGER trg_%1$s_touch BEFORE INSERT OR UPDATE ON %1$s
                        FOR EACH ROW EXECUTE FUNCTION possync_touch()', t);
    END LOOP;
END $$;
