// Package wagering is the use case of the providers: submitting, reading and resuming transactions.
package wagering

import (
	"log/slog"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

// Channel is how a command reached the service.
type Channel string

// The channels.
const (
	ChannelHTTP Channel = "http"
	ChannelSQS  Channel = "sqs"
)

// InboxMessage is the queue message a command came in.
type InboxMessage struct {
	Consumer  string
	MessageID string
	Hash      string
}

// Source is where a command came from.
type Source struct {
	Channel       Channel
	CorrelationID string
	CausationID   string
	Inbox         *InboxMessage
}

// Result is the outcome of a submission.
type Result struct {
	Transaction      wager.Snapshot
	Replay           bool
	DuplicateMessage bool
}

// Metrics records what the service observes. *metrics.Metrics implements it.
type Metrics interface {
	Transaction(channel, kind, status, failureCode string, d time.Duration)
	Duplicate(channel, source string)
	TxRetry(reason string)
}

// Config tunes the service.
type Config struct {
	Reference             wager.ReferencePolicy
	Tx                    retry.Policy
	PermanentFailureLimit int
}

// Service runs the wagering use cases. It is safe for concurrent use.
type Service struct {
	uow     app.UnitOfWork
	clock   app.Clock
	ids     app.IDGenerator
	metrics Metrics
	tracer  tracing.Tracer
	log     *slog.Logger
	cfg     Config
}

// NewService returns a Service that stores through uow and records each submission and resumption as a span on tr.
func NewService(uow app.UnitOfWork, clock app.Clock, ids app.IDGenerator, m Metrics, tr tracing.Tracer, log *slog.Logger, cfg Config) *Service {
	return &Service{uow: uow, clock: clock, ids: ids, metrics: m, tracer: tr, log: log, cfg: cfg}
}
