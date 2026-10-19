// Package outboxrelay publishes the events of the transactional outbox.
package outboxrelay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/failpoint"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

const (
	resultPublished = "published"
	resultFailed    = "failed"
	resultLeaseLost = "lease_lost"
)

const backoffBase = time.Second

const spanPass = "outbox.relay"

const attrClaimed = attribute.Key("outbox.claimed")

var errNoOutcome = errors.New("outboxrelay: the publisher reported no outcome for the event")

// Metrics records what the relay does.
type Metrics interface {
	Published(result string)
	Pending(n int64, oldest time.Duration)
}

// Publisher sends events to the broker and reports the outcome of each by its EventID.
type Publisher interface {
	Publish(ctx context.Context, recs []app.OutboxRecord) map[uuid.UUID]error
}

// Relay moves events from the outbox to the broker, one batch per pass.
type Relay struct {
	uow    app.UnitOfWork
	pub    Publisher
	owner  string
	cfg    config.OutboxConfig
	m      Metrics
	tracer tracing.Tracer
	log    *slog.Logger
}

// New returns a relay that leases events for cfg.Lease under an owner derived from instanceID, tracing on tr.
func New(uow app.UnitOfWork, pub Publisher, instanceID string, cfg config.OutboxConfig, m Metrics, tr tracing.Tracer, log *slog.Logger) *Relay {
	return newRelay(uow, pub, instanceID, cfg, m, tr, log, randomSuffix)
}

func newRelay(uow app.UnitOfWork, pub Publisher, instanceID string, cfg config.OutboxConfig, m Metrics, tr tracing.Tracer,
	log *slog.Logger, suffix func() string) *Relay {
	if log == nil {
		log = slog.Default()
	}
	owner := instanceID + "/" + suffix()
	return &Relay{uow: uow, pub: pub, owner: owner, cfg: cfg, m: m, tracer: tr, log: log.With("outboxOwner", owner)}
}

func randomSuffix() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// RunOnce is one pass of the relay: it publishes the due events and records the outcomes.
func (r *Relay) RunOnce(ctx context.Context) (bool, error) {
	recs, err := r.claim(ctx)
	if err != nil || len(recs) == 0 {
		return false, err
	}
	failpoint.Inject(failpoint.OutboxAfterClaim)

	ctx, span := r.startPass(context.WithoutCancel(ctx), recs)
	defer span.End()
	outcomes := r.publish(ctx, recs)
	failpoint.Inject(failpoint.OutboxAfterPublishBeforeAck)

	if err := r.settle(ctx, recs, outcomes); err != nil {
		tracing.Fail(span, app.ErrorType(err))
		return true, err
	}
	return true, nil
}

func (r *Relay) startPass(ctx context.Context, recs []app.OutboxRecord) (context.Context, trace.Span) {
	if !r.tracer.Enabled() {
		return r.tracer.Start(ctx, spanPass)
	}
	var links []trace.Link
	for _, rec := range recs {
		if sc := tracing.SpanContextFromTraceParent(rec.TraceParent); sc.IsValid() {
			links = append(links, trace.Link{SpanContext: sc})
		}
	}
	return r.tracer.Start(ctx, spanPass, trace.WithNewRoot(), trace.WithLinks(links...),
		trace.WithAttributes(attrClaimed.Int(len(recs))))
}

func (r *Relay) claim(ctx context.Context) ([]app.OutboxRecord, error) {
	var recs []app.OutboxRecord
	err := r.uow.Do(ctx, func(ctx context.Context, repos app.Repos) error {
		var err error
		recs, err = repos.Outbox().ClaimHeads(ctx, r.owner, r.cfg.Lease, r.cfg.BatchSize)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("outboxrelay: claim: %w", err)
	}
	return recs, nil
}

func (r *Relay) publish(ctx context.Context, recs []app.OutboxRecord) map[uuid.UUID]error {
	ctx, cancel := context.WithTimeout(ctx, r.stepTimeout())
	defer cancel()
	return r.pub.Publish(ctx, recs)
}

type settlement struct {
	rec     app.OutboxRecord
	err     error
	retryIn time.Duration
	result  string
}

func (r *Relay) settle(ctx context.Context, recs []app.OutboxRecord, outcomes map[uuid.UUID]error) error {
	ctx, cancel := context.WithTimeout(ctx, r.stepTimeout())
	defer cancel()

	settled := make([]settlement, len(recs))
	for i, rec := range recs {
		err, ok := outcomes[rec.EventID]
		if !ok {
			err = errNoOutcome
		}
		settled[i] = settlement{rec: rec, err: err}
		if err != nil {
			settled[i].retryIn = r.retryIn(rec)
		}
	}

	err := r.uow.Do(ctx, func(ctx context.Context, repos app.Repos) error {
		outbox := repos.Outbox()
		for i := range settled {
			s := &settled[i]
			if s.err != nil {
				held, err := outbox.MarkFailed(ctx, s.rec.EventID, r.owner, s.retryIn, s.err.Error())
				if err != nil {
					return err
				}
				s.result = resultFailed
				if !held {
					s.result = resultLeaseLost
				}
				continue
			}
			acked, err := outbox.MarkPublished(ctx, s.rec.EventID, r.owner)
			if err != nil {
				return err
			}
			s.result = resultPublished
			if !acked {
				s.result = resultLeaseLost
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("outboxrelay: settle %d events: %w", len(recs), err)
	}

	for _, s := range settled {
		r.m.Published(s.result)
		switch {
		case s.result == resultFailed:
			r.log.DebugContext(ctx, "outbox event not published",
				"eventId", s.rec.EventID, "aggregateId", s.rec.AggregateID, "eventType", s.rec.EventType,
				"attempts", s.rec.Attempts, "retryIn", s.retryIn, "error", s.err)
		case s.result == resultLeaseLost && s.err != nil:
			r.log.WarnContext(ctx, "outbox event not published, and another relay took its lease over",
				"eventId", s.rec.EventID, "aggregateId", s.rec.AggregateID, "eventType", s.rec.EventType,
				"attempts", s.rec.Attempts, "error", s.err)
		case s.result == resultLeaseLost:
			r.log.WarnContext(ctx, "outbox event published after another relay took its lease over",
				"eventId", s.rec.EventID, "aggregateId", s.rec.AggregateID, "eventType", s.rec.EventType,
				"attempts", s.rec.Attempts)
		}
	}
	return nil
}

func (r *Relay) retryIn(rec app.OutboxRecord) time.Duration {
	return retry.EqualJitter(retry.Backoff(rec.Attempts, backoffBase, r.cfg.BackoffMax))
}

func (r *Relay) stepTimeout() time.Duration { return r.cfg.Lease / 2 }

// StatsOnce reports the backlog of the outbox to Metrics.Pending.
func (r *Relay) StatsOnce(ctx context.Context) (bool, error) {
	var s app.OutboxStats
	err := r.uow.Snapshot(ctx, func(ctx context.Context, repos app.Repos) error {
		var err error
		s, err = repos.Outbox().Stats(ctx)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("outboxrelay: stats: %w", err)
	}
	r.m.Pending(s.Pending, s.OldestAge)
	return false, nil
}

// Release ends every lease the relay holds.
func (r *Relay) Release(ctx context.Context) error {
	err := r.uow.Do(ctx, func(ctx context.Context, repos app.Repos) error {
		return repos.Outbox().Release(ctx, r.owner)
	})
	if err != nil {
		return fmt.Errorf("outboxrelay: release the leases of %s: %w", r.owner, err)
	}
	return nil
}
