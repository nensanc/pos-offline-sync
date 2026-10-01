-- Local SQLite schema of a node (a store PC).

CREATE TABLE IF NOT EXISTS products (
    id          TEXT PRIMARY KEY,
    sku         TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    price_cents INTEGER NOT NULL DEFAULT 0,
    stock       INTEGER NOT NULL DEFAULT 0,
    -- Server time of the last version seen from the server (NULL = local only).
    updated_at  TEXT
);

CREATE TABLE IF NOT EXISTS sales (
    id               TEXT PRIMARY KEY,
    product_id       TEXT    NOT NULL,
    quantity         INTEGER NOT NULL,
    unit_price_cents INTEGER NOT NULL,
    node_id          TEXT    NOT NULL,
    sold_at          TEXT    NOT NULL,
    updated_at       TEXT
);

-- Transactional outbox: written in the same local transaction as the change it
-- describes, so a change can never be saved without its sync operation.
--   status: pending → synced, or pending → parked after max_attempts data errors.
CREATE TABLE IF NOT EXISTS outbox (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    op_id      TEXT    NOT NULL UNIQUE,
    kind       TEXT    NOT NULL CHECK (kind IN ('upsert', 'delete', 'delta')),
    table_name TEXT    NOT NULL,
    row_id     TEXT    NOT NULL,
    payload    TEXT    NOT NULL DEFAULT '{}',
    status     TEXT    NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'synced', 'parked')),
    attempts   INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    result     TEXT,
    created_at TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_outbox_pending ON outbox (status, seq);
CREATE INDEX IF NOT EXISTS ix_outbox_row ON outbox (table_name, row_id, status);

-- Key/value state: catch-up watermarks per table.
CREATE TABLE IF NOT EXISTS sync_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
