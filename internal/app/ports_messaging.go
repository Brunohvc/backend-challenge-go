package app

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// OutboxRecord is an event as the transactional outbox keeps it; TraceParent is the W3C context it continues.
type OutboxRecord struct {
	EventID       uuid.UUID
	AggregateType string
	AggregateID   string
	EventType     string
	EventVersion  int
	Payload       []byte
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
	Attempts      int
	TraceParent   string
}

// OutboxStats describes what the relay has yet to publish.
type OutboxStats struct {
	Pending   int64
	OldestAge time.Duration
}

// OutboxRepository is the transactional outbox the relays publish from.
type OutboxRepository interface {
	Append(ctx context.Context, recs ...OutboxRecord) error
	ClaimHeads(ctx context.Context, owner string, lease time.Duration, limit int) ([]OutboxRecord, error)
	MarkPublished(ctx context.Context, id uuid.UUID, owner string) (bool, error)
	MarkFailed(ctx context.Context, id uuid.UUID, owner string, retryIn time.Duration, lastErr string) (bool, error)
	Release(ctx context.Context, owner string) error
	Stats(ctx context.Context) (OutboxStats, error)
}

// InboxClaim is what claiming a queue message found in the inbox.
type InboxClaim struct {
	New           bool
	ExistingHash  string
	TransactionID uuid.NullUUID
}

// InboxRepository records the queue messages each consumer has taken on.
type InboxRepository interface {
	Claim(ctx context.Context, consumer, messageID, hash string, now time.Time) (InboxClaim, error)
	Complete(ctx context.Context, consumer, messageID string, txID uuid.NullUUID, outcome string, now time.Time) error
}
