package app

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
)

// NewOutboxRecord marshals an event into the outbox record appended with the change it reports.
func NewOutboxRecord(e event.Envelope) (OutboxRecord, error) {
	id, idOK := canonicalUUID(e.EventID)
	_, aggregateOK := canonicalUUID(e.AggregateID)
	switch {
	case !idOK:
		return OutboxRecord{}, invalidEnvelope("event id must be a canonical non-nil UUID")
	case !aggregateOK:
		return OutboxRecord{}, invalidEnvelope("aggregate id must be a canonical non-nil UUID")
	case e.EventType == "":
		return OutboxRecord{}, invalidEnvelope("event type is required")
	case e.CorrelationID == "":
		return OutboxRecord{}, invalidEnvelope("correlation id is required")
	case e.Version < 1:
		return OutboxRecord{}, invalidEnvelope("version must be at least 1")
	}

	payload, err := json.Marshal(e)
	if err != nil {
		return OutboxRecord{}, fmt.Errorf("app: outbox record: marshal envelope: %w: %w", ErrPermanent, err)
	}

	return OutboxRecord{
		EventID:       id,
		AggregateType: event.AggregateTypeWallet,
		AggregateID:   e.AggregateID,
		EventType:     e.EventType,
		EventVersion:  e.Version,
		Payload:       payload,
		CorrelationID: e.CorrelationID,
		CausationID:   e.CausationID,
		OccurredAt:    time.Time(e.OccurredAt).UTC().Truncate(time.Millisecond),
	}, nil
}

func canonicalUUID(s string) (uuid.UUID, bool) {
	id, err := uuid.Parse(s)
	if err != nil || id == uuid.Nil || id.String() != s {
		return uuid.UUID{}, false
	}
	return id, true
}

func invalidEnvelope(reason string) error {
	return fmt.Errorf("app: outbox record: %w: %s", ErrPermanent, reason)
}
