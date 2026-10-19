package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

const (
	spanDo       = "uow.do"
	spanSnapshot = "uow.snapshot"
)

const attrOutcome = attribute.Key("uow.outcome")

const (
	outcomeCommitted    = "committed"
	outcomeRolledBack   = "rolled_back"
	outcomeBeginFailed  = "begin_failed"
	outcomeCommitFailed = "commit_failed"
	outcomePanicked     = "panicked"
)

// UnitOfWork runs units of work in PostgreSQL transactions on a pool.
type UnitOfWork struct {
	pool   *pgxpool.Pool
	tracer tracing.Tracer
}

var _ app.UnitOfWork = (*UnitOfWork)(nil)

// NewUnitOfWork returns a UnitOfWork running on pool that records each unit as a span on tr.
func NewUnitOfWork(pool *pgxpool.Pool, tr tracing.Tracer) *UnitOfWork {
	return &UnitOfWork{pool: pool, tracer: tr}
}

// Do implements app.UnitOfWork.
func (u *UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context, r app.Repos) error) error {
	return u.run(ctx, spanDo, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite}, fn)
}

// Snapshot implements app.UnitOfWork.
func (u *UnitOfWork) Snapshot(ctx context.Context, fn func(ctx context.Context, r app.Repos) error) error {
	return u.run(ctx, spanSnapshot, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

func (u *UnitOfWork) run(ctx context.Context, name string, opts pgx.TxOptions, fn func(context.Context, app.Repos) error) (err error) {
	ctx, span := u.tracer.Child(ctx, name)
	outcome := outcomePanicked
	defer func() { endUnit(span, outcome, err) }()

	tx, err := u.pool.BeginTx(ctx, opts)
	if err != nil {
		outcome = outcomeBeginFailed
		return fmt.Errorf("postgres: begin: %w", classify(err))
	}
	defer rollback(ctx, tx)

	if err := fn(ctx, repos{tx: tx}); err != nil {
		outcome = outcomeRolledBack
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		outcome = outcomeCommitFailed
		return fmt.Errorf("postgres: commit: %w", classify(err))
	}
	outcome = outcomeCommitted
	return nil
}

func endUnit(span trace.Span, outcome string, err error) {
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(attrOutcome.String(outcome))
	switch {
	case err != nil:
		tracing.Fail(span, app.ErrorType(err))
	case outcome == outcomePanicked:
		tracing.Fail(span, outcomePanicked)
	}
	span.End()
}

func rollback(ctx context.Context, tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), config.RollbackTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

type repos struct {
	tx pgx.Tx
}

func (r repos) Wallets() app.WalletRepository { return walletRepo{q: r.tx} }

func (r repos) Ledger() app.LedgerRepository { return ledgerRepo{q: r.tx} }
