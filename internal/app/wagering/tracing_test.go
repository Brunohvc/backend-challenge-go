package wagering_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/clock/clocktest"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
)

func submitTraced(t *testing.T, f *fixture, in wager.CommandInput, src wagering.Source) (wagering.Result, *tracetest.SpanRecorder, error) {
	t.Helper()
	tr, rec := tracingtest.New(t)
	cmd, err := wager.NewCommand(in)
	if err != nil {
		t.Fatalf("NewCommand(%+v): %v", in, err)
	}
	ctx, root := tracingtest.Root(t.Context(), tr)
	res, err := f.service(tr).Submit(ctx, cmd, src)
	root.End()
	return res, rec, err
}

func requireNoAmount(t *testing.T, s sdktrace.ReadOnlySpan, amounts ...string) {
	t.Helper()
	for key, value := range tracingtest.Attributes(s) {
		for _, word := range []string{"amount", "balance", "money"} {
			if strings.Contains(key, word) {
				t.Errorf("attribute %s names money", key)
			}
		}
		for _, amount := range amounts {
			if strings.Contains(value, amount) {
				t.Errorf("%s = %q carries the amount %s", key, value, amount)
			}
		}
	}
}

func TestSubmit_TracesTheDecisionWithBoundedAttributes(t *testing.T) {
	f := newFixture(t)

	res, rec, err := submitTraced(t, f, f.bet("25.00"), viaHTTP)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	span := tracingtest.One(t, rec, "wagering.submit")
	if !tracingtest.ChildOf(span, tracingtest.One(t, rec, "test")) {
		t.Error("wagering.submit is not under the span of the caller")
	}
	want := map[string]string{
		"wagering.channel":        "http",
		"wagering.kind":           "BET",
		"wagering.status":         "PROCESSED",
		"wagering.provider_id":    "provider-a",
		"wagering.transaction_id": res.Transaction.ID.String(),
		"wallet.id":               f.walletID.String(),
	}
	attrs := tracingtest.Attributes(span)
	for key, value := range want {
		if attrs[key] != value {
			t.Errorf("%s = %q, want %q", key, attrs[key], value)
		}
	}
	for _, absent := range []string{"wagering.failure_code", "wagering.replay", "wagering.duplicate_message"} {
		if _, ok := attrs[absent]; ok {
			t.Errorf("%s = %q on a new processed bet, want it left out", absent, attrs[absent])
		}
	}
	requireNoAmount(t, span, "25.00", "75.00", "100.00")
	if span.Status().Code == codes.Error {
		t.Errorf("Status() = %+v, want no error", span.Status())
	}
}

func TestSubmit_TracesARejectionAndAReplay(t *testing.T) {
	f := newFixture(t)

	_, rec, err := submitTraced(t, f, f.bet("500.00"), viaHTTP)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	rejected := tracingtest.Attributes(tracingtest.One(t, rec, "wagering.submit"))
	if rejected["wagering.status"] != "REJECTED" || rejected["wagering.failure_code"] != "INSUFFICIENT_FUNDS" {
		t.Errorf("status %q, failure code %q; want REJECTED for INSUFFICIENT_FUNDS",
			rejected["wagering.status"], rejected["wagering.failure_code"])
	}

	_, rec, err = submitTraced(t, f, f.bet("500.00"), viaHTTP)
	if err != nil {
		t.Fatalf("Submit() again error = %v", err)
	}
	if replay := tracingtest.Attributes(tracingtest.One(t, rec, "wagering.submit")); replay["wagering.replay"] != "true" {
		t.Errorf("wagering.replay = %q, want true", replay["wagering.replay"])
	}
}

func TestSubmit_TracesTheFailureByItsClass(t *testing.T) {
	f := newFixture(t)
	f.settle(t, f.bet("25.00"), viaHTTP)
	in := f.bet("30.00")

	_, rec, err := submitTraced(t, f, in, viaHTTP)
	if !errors.Is(err, wagering.ErrIdempotencyKeyReused) {
		t.Fatalf("Submit() error = %v, want ErrIdempotencyKeyReused", err)
	}

	span := tracingtest.One(t, rec, "wagering.submit")
	attrs := tracingtest.Attributes(span)
	if span.Status().Code != codes.Error || attrs["error.type"] != "idempotency_key_reused" ||
		span.Status().Description != "idempotency_key_reused" {
		t.Errorf("status %+v, error.type %q; want idempotency_key_reused", span.Status(), attrs["error.type"])
	}
	requireNoAmount(t, span, "30.00", "25.00")
}

func TestSubmit_StartsNoTraceOfItsOwn(t *testing.T) {
	f := newFixture(t)
	tr, rec := tracingtest.New(t)
	cmd, err := wager.NewCommand(f.bet("25.00"))
	if err != nil {
		t.Fatalf("NewCommand: %v", err)
	}

	if _, err := f.service(tr).Submit(context.Background(), cmd, viaHTTP); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	if names := tracingtest.Names(rec); len(names) != 0 {
		t.Errorf("ended spans = %v, want none without a caller span", names)
	}
}

const bug = "bug in a repository"

type panickingUoW struct{}

func (panickingUoW) Do(context.Context, func(context.Context, app.Repos) error) error { panic(bug) }

func (panickingUoW) Snapshot(context.Context, func(context.Context, app.Repos) error) error {
	panic(bug)
}

func TestSubmitAndResume_TraceAPanicAsAFailure(t *testing.T) {
	cmd, err := wager.NewCommand(newFixture(t).bet("25.00"))
	if err != nil {
		t.Fatalf("NewCommand: %v", err)
	}
	tests := []struct {
		span string
		call func(context.Context, *wagering.Service)
	}{
		{"wagering.submit", func(ctx context.Context, svc *wagering.Service) { _, _ = svc.Submit(ctx, cmd, viaHTTP) }},
		{"wagering.resume", func(ctx context.Context, svc *wagering.Service) {
			_, _ = svc.Resume(ctx, app.DueTransaction{ID: idgen.V7{}.New(), WalletID: cmd.WalletID})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.span, func(t *testing.T) {
			tr, rec := tracingtest.New(t)
			svc := wagering.NewService(panickingUoW{}, &clocktest.Fixed{T: submittedAt}, idgen.V7{}, &duplicateMetrics{}, tr,
				slog.New(slog.DiscardHandler), wagering.Config{Tx: retry.Policy{Attempts: 3}})
			ctx, root := tracingtest.Root(t.Context(), tr)

			var recovered any
			func() {
				defer func() { recovered = recover() }()
				tt.call(ctx, svc)
			}()
			root.End()

			if recovered != bug {
				t.Errorf("recovered %v, want the panic to reach the caller unchanged", recovered)
			}
			span := tracingtest.One(t, rec, tt.span)
			attrs := tracingtest.Attributes(span)
			if span.Status().Code != codes.Error || attrs["error.type"] != "panicked" {
				t.Errorf("status %+v, error.type %q; want a failed span of type panicked", span.Status(), attrs["error.type"])
			}
		})
	}
}
