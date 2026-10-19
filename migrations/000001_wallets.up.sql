CREATE TABLE wallets (
    id            UUID        PRIMARY KEY,
    player_id     UUID        NOT NULL,
    currency      CHAR(3)     NOT NULL,
    balance_minor BIGINT      NOT NULL,
    version       BIGINT      NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,

    CONSTRAINT ux_wallets_player_currency UNIQUE (player_id, currency),
    CONSTRAINT ck_wallets_currency CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_wallets_balance CHECK (balance_minor >= 0),
    CONSTRAINT ck_wallets_version CHECK (version >= 1),
    CONSTRAINT ck_wallets_updated_at CHECK (updated_at >= created_at)
);
