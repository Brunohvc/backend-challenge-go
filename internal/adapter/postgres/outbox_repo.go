package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

const lastErrorLimit = 1000

const appendOutboxEvent = `INSERT INTO outbox_events
        (id, aggregate_type, aggregate_id, event_type, event_version, payload,
         correlation_id, causation_id, occurred_at, traceparent, next_attempt_at, created_at)
 VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), $9, NULLIF($10, ''), now(), now())`

const claimOutboxHeads = `WITH RECURSIVE heads AS (
    (SELECT e.aggregate_id, e.seq
       FROM outbox_events e
      WHERE e.published_at IS NULL
      ORDER BY e.aggregate_id, e.seq
      LIMIT 1)
    UNION ALL
    SELECT next_head.aggregate_id, next_head.seq
      FROM heads h
     CROSS JOIN LATERAL (
            SELECT e.aggregate_id, e.seq
              FROM outbox_events e
             WHERE e.published_at IS NULL
               AND e.aggregate_id > h.aggregate_id
             ORDER BY e.aggregate_id, e.seq
             LIMIT 1) next_head
), locked AS MATERIALIZED (
    SELECT due.id
      FROM (SELECT aggregate_id, seq FROM heads ORDER BY seq) h
     CROSS JOIN LATERAL (
            SELECT e.id
              FROM outbox_events e
             WHERE e.aggregate_id = h.aggregate_id
               AND e.seq = h.seq
               AND e.published_at IS NULL
               AND e.next_attempt_at <= now()
               AND (e.locked_until IS NULL OR e.locked_until < now())
               FOR UPDATE SKIP LOCKED) due
     ORDER BY h.seq
     LIMIT $3
), claimed AS (
    UPDATE outbox_events e
       SET locked_by = $1, locked_until = now() + $2::interval, attempts = e.attempts + 1
      FROM locked
     WHERE e.id = locked.id
 RETURNING e.seq, e.id, e.aggregate_type, e.aggregate_id, e.event_type, e.event_version, e.payload,
           e.correlation_id, e.causation_id, e.occurred_at, e.attempts, e.traceparent
)
SELECT id, aggregate_type, aggregate_id, event_type, event_version, payload,
       correlation_id, COALESCE(causation_id, ''), occurred_at, attempts, COALESCE(traceparent, '')
  FROM claimed
 ORDER BY seq`

type outboxRepo struct {
	q querier
}

var _ app.OutboxRepository = outboxRepo{}

func (r repos) Outbox() app.OutboxRepository { return outboxRepo{q: r.tx} }

func (r outboxRepo) Append(ctx context.Context, recs ...app.OutboxRecord) error {
	if len(recs) == 0 {
		return nil
	}
	traceparent := tracing.TraceParent(ctx)
	batch := &pgx.Batch{}
	for _, rec := range recs {
		batch.Queue(appendOutboxEvent,
			rec.EventID, rec.AggregateType, rec.AggregateID, rec.EventType, rec.EventVersion, rec.Payload,
			rec.CorrelationID, rec.CausationID, rec.OccurredAt, traceparent)
	}
	results := r.q.SendBatch(ctx, batch)
	for _, rec := range recs {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("postgres: append outbox event %s: %w", rec.EventID, classify(err))
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("postgres: append %d outbox events: %w", len(recs), classify(err))
	}
	return nil
}

func (r outboxRepo) ClaimHeads(ctx context.Context, owner string, lease time.Duration, limit int) ([]app.OutboxRecord, error) {
	switch {
	case owner == "":
		return nil, fmt.Errorf("postgres: claim outbox events: %w: the owner is empty", app.ErrPermanent)
	case lease <= 0:
		return nil, fmt.Errorf("postgres: claim outbox events for %s: %w: lease %v is not positive",
			owner, app.ErrPermanent, lease)
	case limit < 1:
		return nil, fmt.Errorf("postgres: claim outbox events for %s: %w: limit %d is not positive",
			owner, app.ErrPermanent, limit)
	}
	rows, err := r.q.Query(ctx, claimOutboxHeads, owner, lease, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim outbox events for %s: %w", owner, classify(err))
	}
	recs, err := pgx.CollectRows(rows, scanOutboxRecord)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim outbox events for %s: %w", owner, classify(err))
	}
	return recs, nil
}

func scanOutboxRecord(row pgx.CollectableRow) (app.OutboxRecord, error) {
	var rec app.OutboxRecord
	if err := row.Scan(&rec.EventID, &rec.AggregateType, &rec.AggregateID, &rec.EventType, &rec.EventVersion,
		&rec.Payload, &rec.CorrelationID, &rec.CausationID, &rec.OccurredAt, &rec.Attempts, &rec.TraceParent); err != nil {
		return app.OutboxRecord{}, err
	}
	rec.OccurredAt = rec.OccurredAt.UTC()
	return rec, nil
}

func (r outboxRepo) MarkPublished(ctx context.Context, id uuid.UUID, owner string) (bool, error) {
	tag, err := r.q.Exec(ctx,
		`UPDATE outbox_events
		    SET published_at = now(), locked_by = NULL, locked_until = NULL
		  WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`,
		id, owner)
	if err != nil {
		return false, fmt.Errorf("postgres: mark outbox event %s published by %s: %w", id, owner, classify(err))
	}
	return tag.RowsAffected() == 1, nil
}

func (r outboxRepo) MarkFailed(ctx context.Context, id uuid.UUID, owner string, retryIn time.Duration, lastErr string) (bool, error) {
	if retryIn < 0 {
		return false, fmt.Errorf("postgres: mark outbox event %s failed by %s: %w: retry in %v is negative",
			id, owner, app.ErrPermanent, retryIn)
	}
	tag, err := r.q.Exec(ctx,
		`UPDATE outbox_events
		    SET next_attempt_at = now() + $3::interval, last_error = $4, locked_by = NULL, locked_until = NULL
		  WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`,
		id, owner, retryIn, lastError(lastErr))
	if err != nil {
		return false, fmt.Errorf("postgres: mark outbox event %s failed by %s: %w", id, owner, classify(err))
	}
	return tag.RowsAffected() == 1, nil
}

func lastError(msg string) string {
	msg = strings.ReplaceAll(strings.ToValidUTF8(msg, "�"), "\x00", "")
	n := 0
	for i := range msg {
		if n == lastErrorLimit {
			return msg[:i]
		}
		n++
	}
	return msg
}

func (r outboxRepo) Release(ctx context.Context, owner string) error {
	_, err := r.q.Exec(ctx,
		`UPDATE outbox_events
		    SET locked_by = NULL, locked_until = NULL
		  WHERE locked_by = $1 AND published_at IS NULL`,
		owner)
	if err != nil {
		return fmt.Errorf("postgres: release the outbox leases of %s: %w", owner, classify(err))
	}
	return nil
}

func (r outboxRepo) Stats(ctx context.Context) (app.OutboxStats, error) {
	var s app.OutboxStats
	err := r.q.QueryRow(ctx,
		`SELECT count(*), COALESCE(clock_timestamp() - min(created_at), interval '0')
		   FROM outbox_events
		  WHERE published_at IS NULL`).
		Scan(&s.Pending, &s.OldestAge)
	if err != nil {
		return app.OutboxStats{}, fmt.Errorf("postgres: outbox stats: %w", classify(err))
	}
	return s, nil
}
