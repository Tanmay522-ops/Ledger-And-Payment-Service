CREATE TABLE accounts (
  id         BIGSERIAL PRIMARY KEY,
  kind       TEXT NOT NULL CHECK (kind IN ('user', 'system')),
  balance    BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT user_not_negative CHECK (kind = 'system' OR balance >= 0)
);

CREATE TABLE transfers (
  id           BIGSERIAL PRIMARY KEY,
  from_account BIGINT NOT NULL REFERENCES accounts(id),
  to_account   BIGINT NOT NULL REFERENCES accounts(id),
  amount       BIGINT NOT NULL CHECK (amount > 0 AND amount <= 100000000),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (from_account <> to_account)
);

CREATE TABLE ledger_entries (
  seq         BIGSERIAL PRIMARY KEY,
  transfer_id BIGINT NOT NULL REFERENCES transfers(id),
  account_id  BIGINT NOT NULL REFERENCES accounts(id),
  amount      BIGINT NOT NULL CHECK (amount <> 0),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE idempotency_keys (
  key         TEXT PRIMARY KEY,
  fingerprint TEXT NOT NULL,
  response    JSONB NOT NULL,
  transfer_id BIGINT REFERENCES transfers(id),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at  TIMESTAMPTZ NOT NULL
);

CREATE FUNCTION forbid_change() RETURNS trigger AS $$
BEGIN RAISE EXCEPTION 'ledger_entries is append-only'; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_no_update_delete
  BEFORE UPDATE OR DELETE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION forbid_change();