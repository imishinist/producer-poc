
CREATE TABLE IF NOT EXISTS members (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS member_feed (
    member_id INTEGER PRIMARY KEY,
    last_txid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    last_lsn PG_LSN NOT NULL DEFAULT pg_current_wal_lsn(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
