// Package event defines the integration events published about wallets and their envelope.
package event

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// AggregateTypeWallet is the aggregate type of every event.
const AggregateTypeWallet = "Wallet"

// ErrInvalidMeta marks metadata an envelope cannot be built from.
var ErrInvalidMeta = errors.New("event: invalid meta")

// Meta is the metadata shared by every event.
type Meta struct {
	EventID       uuid.UUID
	WalletID      uuid.UUID
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
}

func (m Meta) validate() error {
	switch {
	case m.EventID == uuid.Nil:
		return invalidMeta("event id is required")
	case m.WalletID == uuid.Nil:
		return invalidMeta("wallet id is required")
	case m.CorrelationID == "":
		return invalidMeta("correlation id is required")
	case m.OccurredAt.IsZero():
		return invalidMeta("occurrence time is required")
	}
	return nil
}

func invalidMeta(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidMeta, reason)
}

// Envelope is the message published for an event, with its typed payload in Data.
type Envelope struct {
	EventID       string    `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateID   string    `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    Timestamp `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          any       `json:"data"`
}

func newEnvelope(eventType string, version int, m Meta, data any) (Envelope, error) {
	if err := m.validate(); err != nil {
		return Envelope{}, err
	}
	return Envelope{
		EventID:       m.EventID.String(),
		EventType:     eventType,
		AggregateID:   m.WalletID.String(),
		CorrelationID: m.CorrelationID,
		CausationID:   m.CausationID,
		OccurredAt:    Timestamp(m.OccurredAt.UTC()),
		Version:       version,
		Data:          data,
	}, nil
}
