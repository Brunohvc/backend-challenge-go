CREATE TABLE wallet_ledger_entries (
    id                   UUID        PRIMARY KEY,
    seq                  BIGINT      GENERATED ALWAYS AS IDENTITY,
    wallet_id            UUID        NOT NULL,
    transaction_id       UUID        NOT NULL,
    direction            TEXT        NOT NULL,
    amount_minor         BIGINT      NOT NULL,
    currency             CHAR(3)     NOT NULL,
    balance_before_minor BIGINT      NOT NULL,
    balance_after_minor  BIGINT      NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL,

    CONSTRAINT ux_ledger_seq UNIQUE (seq),
    CONSTRAINT fk_ledger_wallet FOREIGN KEY (wallet_id) REFERENCES wallets (id),
    CONSTRAINT fk_ledger_transaction FOREIGN KEY (transaction_id) REFERENCES wager_transactions (id),
    CONSTRAINT ux_ledger_wallet_tx UNIQUE (wallet_id, transaction_id),

    CONSTRAINT ck_ledger_direction CHECK (direction IN ('DEBIT', 'CREDIT')),
    CONSTRAINT ck_ledger_amount CHECK (amount_minor > 0),
    CONSTRAINT ck_ledger_currency CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_ledger_balance_before CHECK (balance_before_minor >= 0),
    CONSTRAINT ck_ledger_balance_after CHECK (balance_after_minor >= 0),
    CONSTRAINT ck_ledger_math CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor))
);

CREATE INDEX ix_ledger_wallet_seq ON wallet_ledger_entries (wallet_id, seq);
