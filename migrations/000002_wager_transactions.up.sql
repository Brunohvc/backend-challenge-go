CREATE TABLE wager_transactions (
    id                                UUID        PRIMARY KEY,
    origin                            TEXT        NOT NULL,
    kind                              TEXT        NOT NULL,
    status                            TEXT        NOT NULL,
    wallet_id                         UUID        NOT NULL,
    player_id                         UUID        NOT NULL,
    amount_minor                      BIGINT      NOT NULL,
    currency                          CHAR(3)     NOT NULL,
    provider_id                       TEXT,
    external_transaction_id           TEXT,
    idempotency_key                   TEXT,
    payload_hash                      CHAR(64),
    round_id                          TEXT,
    game_id                           TEXT,
    reference_external_transaction_id TEXT,
    reference_transaction_id          UUID,
    failure_code                      TEXT,
    result_balance_minor              BIGINT,
    result_wallet_version             BIGINT,
    correlation_id                    TEXT        NOT NULL,
    reference_attempts                INT         NOT NULL DEFAULT 0,
    permanent_failures                INT         NOT NULL DEFAULT 0,
    next_attempt_at                   TIMESTAMPTZ,
    expires_at                        TIMESTAMPTZ,
    created_at                        TIMESTAMPTZ NOT NULL,
    updated_at                        TIMESTAMPTZ NOT NULL,
    processed_at                      TIMESTAMPTZ,

    CONSTRAINT fk_tx_wallet FOREIGN KEY (wallet_id) REFERENCES wallets (id),
    CONSTRAINT fk_tx_reference FOREIGN KEY (reference_transaction_id) REFERENCES wager_transactions (id),

    CONSTRAINT ck_tx_origin CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    CONSTRAINT ck_tx_kind CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    CONSTRAINT ck_tx_status CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    CONSTRAINT ck_tx_currency CHECK (currency ~ '^[A-Z]{3}$'),

    CONSTRAINT ck_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL)
        OR (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)),
    CONSTRAINT ck_zero_policy CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)),

    CONSTRAINT ck_reference_required CHECK (
        kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
    CONSTRAINT ck_tx_reference_allowed CHECK (
        kind NOT IN ('BET', 'LOSS') OR reference_external_transaction_id IS NULL),
    CONSTRAINT ck_tx_no_self_reference CHECK (
        reference_external_transaction_id IS NULL OR reference_external_transaction_id <> external_transaction_id),
    CONSTRAINT ck_tx_reference_resolution CHECK (
        reference_transaction_id IS NULL
        OR (reference_external_transaction_id IS NOT NULL AND reference_transaction_id <> id)),
    CONSTRAINT ck_tx_reversal_resolved CHECK (
        status <> 'PROCESSED' OR kind NOT IN ('REFUND', 'ROLLBACK') OR reference_transaction_id IS NOT NULL),

    CONSTRAINT ck_failure_code CHECK ((status IN ('REJECTED', 'FAILED')) = (failure_code IS NOT NULL)),
    CONSTRAINT ck_tx_failure_code_value CHECK (
        failure_code IS NULL
        OR (status = 'FAILED' AND failure_code = 'PROCESSING_FAILED')
        OR (status = 'REJECTED' AND failure_code IN (
            'INSUFFICIENT_FUNDS', 'REVERSAL_INSUFFICIENT_FUNDS', 'CURRENCY_MISMATCH', 'WALLET_PLAYER_MISMATCH',
            'REFERENCE_NOT_FOUND', 'REFERENCE_NOT_PROCESSED', 'REFERENCE_MISMATCH', 'INVALID_REFERENCE_KIND',
            'REVERSAL_AMOUNT_MISMATCH', 'REFERENCE_ALREADY_REVERSED', 'BALANCE_LIMIT_EXCEEDED'))),

    CONSTRAINT ck_pending_schedule CHECK (
        status NOT IN ('PENDING', 'PENDING_REFERENCE') OR next_attempt_at IS NOT NULL),
    CONSTRAINT ck_tx_pending_reference CHECK (
        status <> 'PENDING_REFERENCE'
        OR (reference_external_transaction_id IS NOT NULL AND next_attempt_at IS NOT NULL AND expires_at IS NOT NULL)),
    CONSTRAINT ck_tx_processed_at CHECK (
        (status IN ('PROCESSED', 'REJECTED', 'FAILED')) = (processed_at IS NOT NULL)),

    CONSTRAINT ck_tx_result CHECK (
        (result_balance_minor IS NULL) = (result_wallet_version IS NULL)
        AND (status <> 'PROCESSED' OR result_balance_minor IS NOT NULL)
        AND (result_balance_minor IS NULL
            OR (status IN ('PROCESSED', 'REJECTED') AND result_balance_minor >= 0 AND result_wallet_version >= 1))),

    CONSTRAINT ck_tx_counters CHECK (reference_attempts >= 0 AND permanent_failures >= 0),
    CONSTRAINT ck_tx_updated_at CHECK (updated_at >= created_at),
    CONSTRAINT ck_tx_correlation_id CHECK (correlation_id <> ''),

    CONSTRAINT ck_tx_text_ids CHECK (
        provider_id ~ '^[A-Za-z0-9._:-]{1,128}$'
        AND external_transaction_id ~ '^[A-Za-z0-9._:-]{1,128}$'
        AND round_id ~ '^[A-Za-z0-9._:-]{1,128}$'
        AND game_id ~ '^[A-Za-z0-9._:-]{1,128}$'
        AND reference_external_transaction_id ~ '^[A-Za-z0-9._:-]{1,128}$'),
    CONSTRAINT ck_tx_idempotency_key CHECK (idempotency_key ~ '^[!-~]{1,255}$'),
    CONSTRAINT ck_tx_payload_hash CHECK (payload_hash ~ '^[0-9a-f]{64}$')
);

CREATE UNIQUE INDEX ux_tx_provider_key ON wager_transactions (provider_id, idempotency_key)
    WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX ux_tx_provider_external ON wager_transactions (provider_id, external_transaction_id)
    WHERE origin = 'EXTERNAL';

CREATE UNIQUE INDEX ux_tx_single_opening ON wager_transactions (wallet_id)
    WHERE kind = 'OPENING';

CREATE UNIQUE INDEX ux_tx_single_reversal ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');

CREATE INDEX ix_tx_due ON wager_transactions (next_attempt_at)
    WHERE status IN ('PENDING', 'PENDING_REFERENCE');
