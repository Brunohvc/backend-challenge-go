CREATE INDEX CONCURRENTLY ix_outbox_due ON outbox_events (next_attempt_at, seq) WHERE published_at IS NULL;
