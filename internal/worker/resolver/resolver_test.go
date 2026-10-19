package resolver_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/metrics"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
	"github.com/Brunohvc/backend-challenge-go/internal/worker"
	"github.com/Brunohvc/backend-challenge-go/internal/worker/resolver"
)

var (
	_ resolver.ResumeService = (*wagering.Service)(nil)
	_ resolver.Metrics       = (*metrics.Metrics)(nil)
	_ worker.Task            = (*resolver.Resolver)(nil).RunOnce
)

var unitConfig = config.ResolverConfig{
	BatchSize: 7,
	Lease:     42 * time.Second,
}

var (
	errPermanent   = fmt.Errorf("wagering: resume transaction: %w: a broken invariant", app.ErrPermanent)
	errUnavailable = fmt.Errorf("postgres: begin: %w: connection refused", app.ErrUnavailable)
	errNotCounted  = fmt.Errorf("%w (the failure could not be counted: %w)", errPermanent, errUnavailable)
)

func newID() uuid.UUID { return idgen.V7{}.New() }

type resumption struct {
	outcome wagering.ResumeOutcome
	err     error
}

type fakeService struct {
	claimed     []app.DueTransaction
	claimErr    error
	resumptions map[uuid.UUID]resumption
	onClaim     func()
	onResume    func(ctx context.Context, due app.DueTransaction)

	lease       time.Duration
	limit       int
	resumed     []uuid.UUID
	resumedDone []bool
}

func newFakeService(rs ...resumption) *fakeService {
	f := &fakeService{resumptions: make(map[uuid.UUID]resumption)}
	for _, r := range rs {
		due := app.DueTransaction{ID: newID(), WalletID: newID(), LeaseUntil: time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)}
		f.claimed = append(f.claimed, due)
		f.resumptions[due.ID] = r
	}
	return f
}

func (f *fakeService) ClaimDue(_ context.Context, lease time.Duration, limit int) ([]app.DueTransaction, error) {
	f.lease, f.limit = lease, limit
	if f.onClaim != nil {
		f.onClaim()
	}
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	return f.claimed, nil
}

func (f *fakeService) Resume(ctx context.Context, due app.DueTransaction) (wagering.ResumeOutcome, error) {
	f.resumed = append(f.resumed, due.ID)
	if f.onResume != nil {
		f.onResume(ctx, due)
	}
	f.resumedDone = append(f.resumedDone, ctx.Err() != nil)
	r := f.resumptions[due.ID]
	return r.outcome, r.err
}

func (f *fakeService) ids() []uuid.UUID {
	ids := make([]uuid.UUID, len(f.claimed))
	for i, due := range f.claimed {
		ids[i] = due.ID
	}
	return ids
}

type recordedResolutions struct {
	mu      sync.Mutex
	results []string
}

func (m *recordedResolutions) Resolution(result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.results = append(m.results, result)
}

func (m *recordedResolutions) all() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.results)
}

func logRecords(t *testing.T, buf *bytes.Buffer, level slog.Level) []map[string]any {
	t.Helper()
	var got []map[string]any
	for line := range bytes.Lines(buf.Bytes()) {
		var rec struct {
			Level slog.Level `json:"level"`
		}
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if rec.Level < level {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		got = append(got, fields)
	}
	return got
}

func newResolver(svc resolver.ResumeService) (*resolver.Resolver, *recordedResolutions, *bytes.Buffer) {
	counted := &recordedResolutions{}
	logs := &bytes.Buffer{}
	return resolver.New(svc, unitConfig, counted, tracing.Tracer{}, logging.New(logs, slog.LevelDebug, "test")), counted, logs
}

func runOnce(t *testing.T, r *resolver.Resolver) {
	t.Helper()
	didWork, err := r.RunOnce(t.Context())
	if err != nil || !didWork {
		t.Fatalf("RunOnce() = %t, %v; want true, nil", didWork, err)
	}
}

func TestRunOnce_ResumesEveryClaimInClaimOrder(t *testing.T) {
	svc := newFakeService(
		resumption{outcome: wagering.ResumeProcessed},
		resumption{outcome: wagering.ResumeRescheduled},
		resumption{outcome: wagering.ResumeRejected},
	)
	r, _, _ := newResolver(svc)

	runOnce(t, r)

	if svc.lease != unitConfig.Lease || svc.limit != unitConfig.BatchSize {
		t.Errorf("ClaimDue(lease %s, limit %d), want lease %s, limit %d", svc.lease, svc.limit, unitConfig.Lease, unitConfig.BatchSize)
	}
	if !slices.Equal(svc.resumed, svc.ids()) {
		t.Errorf("resumed %v, want %v, the claim order", svc.resumed, svc.ids())
	}
}

func TestRunOnce_CountsEveryResolutionButASkip(t *testing.T) {
	tests := []struct {
		name string
		resumption
		want []string
	}{
		{"processed", resumption{outcome: wagering.ResumeProcessed}, []string{"processed"}},
		{"rejected", resumption{outcome: wagering.ResumeRejected}, []string{"rejected"}},
		{"rescheduled", resumption{outcome: wagering.ResumeRescheduled}, []string{"rescheduled"}},
		{"failed", resumption{outcome: wagering.ResumeFailed, err: errPermanent}, []string{"failed"}},
		{"skipped", resumption{outcome: wagering.ResumeSkipped}, nil},
		{"error", resumption{err: errPermanent}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, counted, _ := newResolver(newFakeService(tt.resumption))

			runOnce(t, r)

			if !slices.Equal(counted.all(), tt.want) {
				t.Errorf("counted %q, want %q", counted.all(), tt.want)
			}
		})
	}
}

func TestRunOnce_GoesOnAfterATransactionFails(t *testing.T) {
	svc := newFakeService(resumption{err: errPermanent}, resumption{outcome: wagering.ResumeProcessed})
	r, counted, logs := newResolver(svc)

	runOnce(t, r)

	if !slices.Equal(svc.resumed, svc.ids()) {
		t.Errorf("resumed %v, want every claim, %v", svc.resumed, svc.ids())
	}
	if !slices.Equal(counted.all(), []string{"processed"}) {
		t.Errorf("counted %q, want only the processed transaction", counted.all())
	}
	warns := logRecords(t, logs, slog.LevelWarn)
	if len(warns) != 1 {
		t.Fatalf("logged %v at WARN or above, want the failure alone", warns)
	}
	failed := svc.claimed[0]
	if warns[0]["level"] != slog.LevelWarn.String() ||
		warns[0]["transactionId"] != failed.ID.String() || warns[0]["walletId"] != failed.WalletID.String() ||
		warns[0]["error"] != errPermanent.Error() {
		t.Errorf("logged %v, want a WARN naming transaction %s of wallet %s with the error %q",
			warns[0], failed.ID, failed.WalletID, errPermanent)
	}
}

func TestRunOnce_ReportsATransactionThatKeepsFailingOnEveryClaim(t *testing.T) {
	svc := newFakeService(resumption{err: errPermanent})
	r, _, logs := newResolver(svc)

	runOnce(t, r)
	runOnce(t, r)

	if warns := logRecords(t, logs, slog.LevelWarn); len(warns) != 2 {
		t.Errorf("logged %d records at WARN or above, want one per claim, 2", len(warns))
	}
}

func TestRunOnce_StopsAtAnUnavailableDatabase(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"unavailable", errUnavailable},
		{"permanent, and its count not written", errNotCounted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newFakeService(
				resumption{outcome: wagering.ResumeProcessed},
				resumption{err: tt.err},
				resumption{outcome: wagering.ResumeProcessed},
			)
			r, counted, logs := newResolver(svc)

			didWork, err := r.RunOnce(t.Context())

			if !errors.Is(err, app.ErrUnavailable) || !errors.Is(err, tt.err) {
				t.Fatalf("RunOnce() error = %v, want one wrapping app.ErrUnavailable and %v", err, tt.err)
			}
			if !didWork {
				t.Error("RunOnce() reported no work, want work: it claimed transactions")
			}
			if want := svc.ids()[:2]; !slices.Equal(svc.resumed, want) {
				t.Errorf("resumed %v, want %v: nothing after the unavailable database", svc.resumed, want)
			}
			if !slices.Equal(counted.all(), []string{"processed"}) {
				t.Errorf("counted %q, want only the processed transaction", counted.all())
			}
			if warns := logRecords(t, logs, slog.LevelWarn); len(warns) != 0 {
				t.Errorf("logged %v at WARN or above, want nothing", warns)
			}
		})
	}
}

func TestRunOnce_StopsBetweenTransactionsOnShutdown(t *testing.T) {
	tests := []struct {
		name        string
		interrupt   func(svc *fakeService, cancel context.CancelFunc)
		wantResumed int
	}{
		{
			name:        "while claiming",
			interrupt:   func(svc *fakeService, cancel context.CancelFunc) { svc.onClaim = cancel },
			wantResumed: 0,
		},
		{
			name: "while resuming",
			interrupt: func(svc *fakeService, cancel context.CancelFunc) {
				svc.onResume = func(context.Context, app.DueTransaction) { cancel() }
			},
			wantResumed: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newFakeService(resumption{outcome: wagering.ResumeProcessed}, resumption{outcome: wagering.ResumeProcessed})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			tt.interrupt(svc, cancel)
			r, counted, _ := newResolver(svc)

			didWork, err := r.RunOnce(ctx)

			if !errors.Is(err, context.Canceled) || !didWork {
				t.Fatalf("RunOnce() = %t, %v; want true and an error wrapping context.Canceled", didWork, err)
			}
			if want := svc.ids()[:tt.wantResumed]; !slices.Equal(svc.resumed, want) {
				t.Errorf("resumed %v, want %v", svc.resumed, want)
			}
			if slices.Contains(svc.resumedDone, true) {
				t.Error("the shutdown canceled the resumption under way, want it to finish")
			}
			if len(counted.all()) != tt.wantResumed {
				t.Errorf("counted %q, want %d resolutions", counted.all(), tt.wantResumed)
			}
		})
	}
}

func TestRunOnce_BoundsAResumptionByTheLease(t *testing.T) {
	svc := newFakeService(resumption{outcome: wagering.ResumeProcessed})
	var (
		deadline    time.Time
		hasDeadline bool
	)
	svc.onResume = func(ctx context.Context, _ app.DueTransaction) { deadline, hasDeadline = ctx.Deadline() }
	r, _, _ := newResolver(svc)
	before := time.Now()

	runOnce(t, r)

	if !hasDeadline || deadline.After(time.Now().Add(unitConfig.Lease)) || deadline.Before(before.Add(unitConfig.Lease)) {
		t.Errorf("resumed with deadline %v (set: %t), want one lease, %s, after the resumption began", deadline, hasDeadline, unitConfig.Lease)
	}
}

func TestRunOnce_ReturnsTheClaimError(t *testing.T) {
	svc := newFakeService(resumption{outcome: wagering.ResumeProcessed})
	svc.claimErr = errUnavailable
	r, counted, _ := newResolver(svc)

	didWork, err := r.RunOnce(t.Context())

	if !errors.Is(err, errUnavailable) || didWork {
		t.Fatalf("RunOnce() = %t, %v; want false, %v", didWork, err, errUnavailable)
	}
	if len(svc.resumed) != 0 || len(counted.all()) != 0 {
		t.Errorf("resumed %v and counted %q, want nothing", svc.resumed, counted.all())
	}
}

func TestRunOnce_NothingDue(t *testing.T) {
	r, counted, logs := newResolver(newFakeService())

	didWork, err := r.RunOnce(t.Context())

	if didWork || err != nil {
		t.Fatalf("RunOnce() = %t, %v; want false, nil, so the runner waits for the next poll", didWork, err)
	}
	if len(counted.all()) != 0 || logs.Len() != 0 {
		t.Errorf("counted %q and logged %q, want nothing", counted.all(), logs)
	}
}

func TestRunOnce_DoesNotLogAFailedTransactionAgain(t *testing.T) {
	r, counted, logs := newResolver(newFakeService(resumption{outcome: wagering.ResumeFailed, err: errPermanent}))

	runOnce(t, r)

	if logs.Len() != 0 {
		t.Errorf("logged %q, want nothing", logs)
	}
	if !slices.Equal(counted.all(), []string{"failed"}) {
		t.Errorf("counted %q, want the failure", counted.all())
	}
}

func TestRunOnce_TracesThePassThatFoundWork(t *testing.T) {
	svc := newFakeService(resumption{outcome: wagering.ResumeProcessed}, resumption{outcome: wagering.ResumeRescheduled})
	var resumedUnder []trace.SpanContext
	svc.onResume = func(ctx context.Context, _ app.DueTransaction) {
		resumedUnder = append(resumedUnder, trace.SpanContextFromContext(ctx))
	}
	tr, rec := tracingtest.New(t)
	r := resolver.New(svc, unitConfig, &recordedResolutions{}, tr, slog.New(slog.DiscardHandler))

	runOnce(t, r)

	pass := tracingtest.One(t, rec, "resolver.pass")
	if pass.Parent().IsValid() {
		t.Errorf("the pass is under %s, want it to start a trace", pass.Parent().SpanID())
	}
	if attrs := tracingtest.Attributes(pass); attrs["resolver.claimed"] != "2" {
		t.Errorf("resolver.claimed = %q, want 2", attrs["resolver.claimed"])
	}
	if len(resumedUnder) != 2 || !resumedUnder[0].Equal(pass.SpanContext()) || !resumedUnder[1].Equal(pass.SpanContext()) {
		t.Errorf("resumed under %v, want every resumption under the pass", resumedUnder)
	}
}

func TestRunOnce_TracesNothingWithoutWork(t *testing.T) {
	tr, rec := tracingtest.New(t)
	r := resolver.New(newFakeService(), unitConfig, &recordedResolutions{}, tr, slog.New(slog.DiscardHandler))

	if _, err := r.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	if names := tracingtest.Names(rec); len(names) != 0 {
		t.Errorf("ended spans = %v, want none for a pass that claimed nothing", names)
	}
}

func TestRunOnce_PassRecordsAnUnavailableDatabase(t *testing.T) {
	svc := newFakeService(resumption{err: errUnavailable})
	tr, rec := tracingtest.New(t)
	r := resolver.New(svc, unitConfig, &recordedResolutions{}, tr, slog.New(slog.DiscardHandler))

	if _, err := r.RunOnce(t.Context()); !errors.Is(err, app.ErrUnavailable) {
		t.Fatalf("RunOnce() error = %v, want app.ErrUnavailable", err)
	}

	pass := tracingtest.One(t, rec, "resolver.pass")
	if attrs := tracingtest.Attributes(pass); pass.Status().Code != codes.Error || attrs["error.type"] != "unavailable" {
		t.Errorf("status %+v, error.type %q; want an unavailable error", pass.Status(), attrs["error.type"])
	}
}
