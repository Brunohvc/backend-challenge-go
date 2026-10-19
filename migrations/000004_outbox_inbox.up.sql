CREATE TABLE outbox_events (
    id              UUID        PRIMARY KEY,
    seq             BIGINT      GENERATED ALWAYS AS IDENTITY,
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    event_version   INT         NOT NULL,
    payload         JSONB       NOT NULL,
    correlation_id  TEXT        NOT NULL,
    causation_id    TEXT,
    occurred_at     TIMESTAMPTZ NOT NULL,
    attempts        INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    last_error      TEXT,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL,

    CONSTRAINT ux_outbox_seq UNIQUE (seq),
    CONSTRAINT ck_outbox_event_version CHECK (event_version >= 1),
    CONSTRAINT ck_outbox_attempts CHECK (attempts >= 0),
    CONSTRAINT ck_outbox_payload CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX ix_outbox_due ON outbox_events (next_attempt_at, seq) WHERE published_at IS NULL;
CREATE INDEX ix_outbox_aggregate ON outbox_events (aggregate_id, seq) WHERE published_at IS NULL;

CREATE TABLE inbox_messages (
    consumer_name  TEXT        NOT NULL,
    message_id     TEXT        NOT NULL,
    payload_hash   CHAR(64)    NOT NULL,
    received_at    TIMESTAMPTZ NOT NULL,
    processed_at   TIMESTAMPTZ,
    transaction_id UUID,
    outcome        TEXT,

    PRIMARY KEY (consumer_name, message_id),
    CONSTRAINT fk_inbox_transaction FOREIGN KEY (transaction_id) REFERENCES wager_transactions (id),
    CONSTRAINT ck_inbox_payload_hash CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_inbox_outcome CHECK (outcome IN ('PROCESSED', 'REJECTED', 'PENDING_REFERENCE', 'REPLAY')),
    CONSTRAINT ck_inbox_completed CHECK ((processed_at IS NULL) = (outcome IS NULL))
);
