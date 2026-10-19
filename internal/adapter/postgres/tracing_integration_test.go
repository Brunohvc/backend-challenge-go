//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
)

func newTracedUnitOfWork(t *testing.T, tr tracing.Tracer) *postgres.UnitOfWork {
	t.Helper()
	pool, err := postgres.NewPool(t.Context(), dbConfig(), "test", postgres.WithTracing(tr))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(pool.Close)
	return postgres.NewUnitOfWork(pool, tr)
}

func childrenOf(spans []sdktrace.ReadOnlySpan, parent sdktrace.ReadOnlySpan) []string {
	var names []string
	for _, s := range spans {
		if tracingtest.ChildOf(s, parent) {
			names = append(names, s.Name())
		}
	}
	return names
}

func TestUnitOfWork_TracesItsStatements(t *testing.T) {
	type unit = func(*postgres.UnitOfWork, context.Context, func(context.Context, app.Repos) error) error
	do, snapshot := unit((*postgres.UnitOfWork).Do), unit((*postgres.UnitOfWork).Snapshot)
	errRefused := errors.New("refused with 25.00")
	tests := []struct {
		name     string
		run      unit
		fnErr    error
		span     string
		outcome  string
		children []string
	}{
		{"a committed unit", do, nil, "uow.do", "committed", []string{"BEGIN", "SELECT wallets", "COMMIT"}},
		{"a snapshot", snapshot, nil, "uow.snapshot", "committed", []string{"BEGIN", "SELECT wallets", "COMMIT"}},
		{"a refused unit", do, errRefused, "uow.do", "rolled_back", []string{"BEGIN", "SELECT wallets", "ROLLBACK"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, rec := tracingtest.New(t)
			uow := newTracedUnitOfWork(t, tr)
			ctx, root := tracingtest.Root(t.Context(), tr)

			err := tt.run(uow, ctx, func(ctx context.Context, r app.Repos) error {
				if _, err := r.Wallets().Get(ctx, newID()); !errors.Is(err, app.ErrNotFound) {
					t.Errorf("Get() error = %v, want app.ErrNotFound", err)
				}
				return tt.fnErr
			})
			root.End()

			if !errors.Is(err, tt.fnErr) {
				t.Fatalf("unit of work error = %v, want %v", err, tt.fnErr)
			}
			unit := tracingtest.One(t, rec, tt.span)
			if !tracingtest.ChildOf(unit, tracingtest.One(t, rec, "test")) {
				t.Errorf("%s is not under the span of the caller", tt.span)
			}
			if got := childrenOf(rec.Ended(), unit); !slices.Equal(got, tt.children) {
				t.Errorf("statements under %s = %v, want %v", tt.span, got, tt.children)
			}
			attrs := tracingtest.Attributes(unit)
			if attrs["uow.outcome"] != tt.outcome {
				t.Errorf("uow.outcome = %q, want %q", attrs["uow.outcome"], tt.outcome)
			}
			if failed := unit.Status().Code == codes.Error; failed != (tt.fnErr != nil) {
				t.Errorf("status = %+v, want an error status only for a refused unit", unit.Status())
			}
			if tt.fnErr != nil && (attrs["error.type"] != "other" || unit.Status().Description != "other") {
				t.Errorf("error.type = %q, status %q; want the bounded class, never the message",
					attrs["error.type"], unit.Status().Description)
			}
		})
	}
}

func TestUnitOfWork_TracesAPanic(t *testing.T) {
	tr, rec := tracingtest.New(t)
	uow := newTracedUnitOfWork(t, tr)
	ctx, root := tracingtest.Root(t.Context(), tr)

	func() {
		defer func() { _ = recover() }()
		_ = uow.Do(ctx, func(context.Context, app.Repos) error { panic("bug in the use case") })
	}()
	root.End()

	unit := tracingtest.One(t, rec, "uow.do")
	attrs := tracingtest.Attributes(unit)
	if attrs["uow.outcome"] != "panicked" || unit.Status().Code != codes.Error || attrs["error.type"] != "panicked" {
		t.Errorf("outcome %q, status %+v, error.type %q; want a failed panicked unit",
			attrs["uow.outcome"], unit.Status(), attrs["error.type"])
	}
	if got := childrenOf(rec.Ended(), unit); !slices.Equal(got, []string{"BEGIN", "ROLLBACK"}) {
		t.Errorf("statements under the unit = %v, want it begun and rolled back", got)
	}
}

func TestUnitOfWork_StartsNoTraceOfItsOwn(t *testing.T) {
	tr, rec := tracingtest.New(t)
	uow := newTracedUnitOfWork(t, tr)

	err := uow.Do(t.Context(), func(ctx context.Context, _ app.Repos) error {
		if tracing.TraceParent(ctx) != "" {
			t.Error("the unit of work started a span without a caller recording one")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if names := tracingtest.Names(rec); len(names) != 0 {
		t.Errorf("ended spans = %v, want none without a caller span", names)
	}
}

func TestNewPool_TracesOnlyWhenAsked(t *testing.T) {
	pool := newPool(t, dbConfig())
	tr, _ := tracingtest.New(t)
	traced, err := postgres.NewPool(t.Context(), dbConfig(), "test", postgres.WithTracing(tr))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(traced.Close)

	for name, p := range map[string]*pgxpool.Pool{"plain": pool, "traced": traced} {
		if got, want := p.Config().ConnConfig.Tracer != nil, name == "traced"; got != want {
			t.Errorf("%s pool has a tracer = %t, want %t", name, got, want)
		}
	}
}

func TestOutboxTraceParentCheck_LeavesTheRowsAlreadyThereUnscanned(t *testing.T) {
	owner := connect(t, pg.OwnerDSN)
	var validated bool
	err := owner.QueryRow(t.Context(),
		"SELECT convalidated FROM pg_constraint WHERE contype = 'c' AND conname = 'ck_outbox_traceparent'").Scan(&validated)
	if err != nil {
		t.Fatalf("read ck_outbox_traceparent: %v", err)
	}
	if validated {
		t.Error("ck_outbox_traceparent was checked against the rows already in the outbox, want it added NOT VALID")
	}
}
