// Package resolver resumes in the background the transactions that wait for a referenced one.
package resolver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/failpoint"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

const spanPass = "resolver.pass"

const attrClaimed = attribute.Key("resolver.claimed")

// ResumeService is the part of the wagering use case the resolver drives.
type ResumeService interface {
	ClaimDue(ctx context.Context, lease time.Duration, limit int) ([]app.DueTransaction, error)
	Resume(ctx context.Context, due app.DueTransaction) (wagering.ResumeOutcome, error)
}

// Metrics records what the resolver observes.
type Metrics interface {
	Resolution(result string)
}

// Resolver resumes the transactions that are due, a batch per pass.
type Resolver struct {
	svc     ResumeService
	lease   time.Duration
	limit   int
	metrics Metrics
	tracer  tracing.Tracer
	log     *slog.Logger
}

// New returns a Resolver that claims at most cfg.BatchSize transactions per pass and traces each pass on tr.
func New(svc ResumeService, cfg config.ResolverConfig, m Metrics, tr tracing.Tracer, log *slog.Logger) *Resolver {
	if log == nil {
		log = slog.Default()
	}
	return &Resolver{svc: svc, lease: cfg.Lease, limit: cfg.BatchSize, metrics: m, tracer: tr, log: log}
}

// RunOnce is one pass of the resolver: it resumes the transactions that are due.
func (r *Resolver) RunOnce(ctx context.Context) (didWork bool, err error) {
	claimed, err := r.svc.ClaimDue(ctx, r.lease, r.limit)
	if err != nil {
		return false, err
	}
	if len(claimed) == 0 {
		return false, nil
	}
	failpoint.Inject(failpoint.ResolverAfterClaim)

	ctx, span := r.startPass(ctx, len(claimed))
	defer span.End()
	if err := r.resumeAll(ctx, claimed); err != nil {
		tracing.Fail(span, app.ErrorType(err))
		return true, err
	}
	return true, nil
}

func (r *Resolver) startPass(ctx context.Context, claimed int) (context.Context, trace.Span) {
	if !r.tracer.Enabled() {
		return r.tracer.Start(ctx, spanPass)
	}
	return r.tracer.Start(ctx, spanPass, trace.WithNewRoot(), trace.WithAttributes(attrClaimed.Int(claimed)))
}

func (r *Resolver) resumeAll(ctx context.Context, claimed []app.DueTransaction) error {
	for i, due := range claimed {
		stop := ctx.Err()
		if stop == nil {
			stop = r.resume(ctx, due)
		}
		if stop != nil {
			return fmt.Errorf("resolver: %d of %d claimed transactions left to wait out their lease: %w",
				len(claimed)-i, len(claimed), stop)
		}
	}
	return nil
}

func (r *Resolver) resume(ctx context.Context, due app.DueTransaction) error {
	ctx = logging.With(ctx, logging.Fields{TransactionID: due.ID.String(), WalletID: due.WalletID.String()})

	resumeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.lease)
	defer cancel()
	outcome, err := r.svc.Resume(resumeCtx, due)

	if counted(outcome) {
		r.metrics.Resolution(string(outcome))
	}
	switch {
	case err == nil:
		return nil
	case outcome == wagering.ResumeFailed:
		return nil
	case errors.Is(err, app.ErrUnavailable):
		return err
	default:
		r.log.WarnContext(ctx, "transaction not resumed, to be tried again once its lease runs out",
			slog.String("error", err.Error()))
		return nil
	}
}

func counted(outcome wagering.ResumeOutcome) bool {
	switch outcome {
	case wagering.ResumeProcessed, wagering.ResumeRejected, wagering.ResumeRescheduled, wagering.ResumeFailed:
		return true
	}
	return false
}
