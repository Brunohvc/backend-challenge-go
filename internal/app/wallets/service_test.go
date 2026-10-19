package wallets_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wallets"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/clock/clocktest"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
)

var t0 = time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)

type calls []string

type fakeUoW struct {
	repos     *fakeRepos
	doCalls   int
	snapshots int
}

func (u *fakeUoW) Do(ctx context.Context, fn func(context.Context, app.Repos) error) error {
	u.doCalls++
	return fn(ctx, u.repos)
}

func (u *fakeUoW) Snapshot(ctx context.Context, fn func(context.Context, app.Repos) error) error {
	u.snapshots++
	u.repos.readOnly = true
	defer func() { u.repos.readOnly = false }()
	return fn(ctx, u.repos)
}

type fakeRepos struct {
	t        *testing.T
	log      calls
	readOnly bool

	insertWalletErr error
	lookupErr       error
	stored          map[uuid.UUID]wallet.Snapshot
	page            []app.LedgerRecord
	pageLimit       int
	totals          app.LedgerTotals
	outbox          []app.OutboxRecord
}

func newFakeRepos(t *testing.T) *fakeRepos {
	return &fakeRepos{t: t, stored: map[uuid.UUID]wallet.Snapshot{}}
}

func (r *fakeRepos) write(what string) {
	r.t.Helper()
	if r.readOnly {
		r.t.Errorf("%s inside a read-only unit of work", what)
	}
	r.log = append(r.log, what)
}

func (r *fakeRepos) Wallets() app.WalletRepository           { return fakeWallets{r} }
func (r *fakeRepos) Ledger() app.LedgerRepository            { return fakeLedger{r} }
func (r *fakeRepos) Transactions() app.TransactionRepository { return fakeTransactions{r: r} }
func (r *fakeRepos) Outbox() app.OutboxRepository            { return fakeOutbox{r: r} }
func (r *fakeRepos) Inbox() app.InboxRepository              { return nil }

type fakeWallets struct{ r *fakeRepos }

func (f fakeWallets) Insert(_ context.Context, w *wallet.Wallet) error {
	f.r.write("wallets.Insert")
	if f.r.insertWalletErr != nil {
		return f.r.insertWalletErr
	}
	f.r.stored[w.ID()] = w.Snapshot()
	return nil
}

func (f fakeWallets) Get(_ context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	s, ok := f.r.stored[id]
	if !ok {
		return nil, fmt.Errorf("fake: wallet %s: %w", id, app.ErrNotFound)
	}
	return wallet.Rehydrate(s)
}

func (f fakeWallets) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return f.Get(ctx, id)
}

func (f fakeWallets) IDByPlayerAndCurrency(_ context.Context, playerID uuid.UUID, c money.Currency) (uuid.UUID, error) {
	if f.r.lookupErr != nil {
		return uuid.Nil, f.r.lookupErr
	}
	for id, s := range f.r.stored {
		if s.PlayerID == playerID && s.Balance.Currency() == c {
			return id, nil
		}
	}
	return uuid.Nil, fmt.Errorf("fake: wallet of player %s: %w", playerID, app.ErrNotFound)
}

func (f fakeWallets) UpdateBalance(context.Context, *wallet.Wallet, int64) error {
	f.r.write("wallets.UpdateBalance")
	return nil
}

type fakeLedger struct{ r *fakeRepos }

func (f fakeLedger) Append(context.Context, wallet.LedgerEntry) error {
	f.r.write("ledger.Append")
	return nil
}

func (f fakeLedger) Page(_ context.Context, _ uuid.UUID, afterSeq int64, limit int) ([]app.LedgerRecord, error) {
	f.r.pageLimit = limit
	var out []app.LedgerRecord
	for _, rec := range f.r.page {
		if rec.Seq > afterSeq && len(out) < limit {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (f fakeLedger) Totals(context.Context, uuid.UUID) (app.LedgerTotals, error) {
	return f.r.totals, nil
}

type fakeTransactions struct {
	app.TransactionRepository
	r *fakeRepos
}

func (f fakeTransactions) Insert(context.Context, *wager.Transaction) error {
	f.r.write("transactions.Insert")
	return nil
}

type fakeOutbox struct {
	app.OutboxRepository
	r *fakeRepos
}

func (f fakeOutbox) Append(_ context.Context, recs ...app.OutboxRecord) error {
	f.r.write("outbox.Append")
	f.r.outbox = append(f.r.outbox, recs...)
	return nil
}

type countingMetrics struct {
	mu          sync.Mutex
	divergences int
	retries     []string
}

func (m *countingMetrics) ReconciliationDivergence() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.divergences++
}

func (m *countingMetrics) TxRetry(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retries = append(m.retries, reason)
}

var txPolicy = retry.Policy{Attempts: 3, Base: time.Millisecond, Max: time.Millisecond, Jitter: func(time.Duration) time.Duration { return 0 }}

type fixture struct {
	svc     *wallets.Service
	uow     *fakeUoW
	repos   *fakeRepos
	metrics *countingMetrics
	logs    *bytes.Buffer
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	repos := newFakeRepos(t)
	f := &fixture{uow: &fakeUoW{repos: repos}, repos: repos, metrics: &countingMetrics{}, logs: &bytes.Buffer{}}
	log := logging.New(f.logs, slog.LevelDebug, "test")
	f.svc = wallets.NewService(f.uow, &clocktest.Fixed{T: t0}, idgen.V7{}, f.metrics, log, txPolicy)
	return f
}

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		t.Fatalf("money %s: %v", amount, err)
	}
	return m
}

func storeWallet(t *testing.T, f *fixture, player uuid.UUID, currency, amount string) uuid.UUID {
	t.Helper()
	balance, err := money.Parse(amount, currency)
	if err != nil {
		t.Fatalf("money %s %s: %v", amount, currency, err)
	}
	id := idgen.V7{}.New()
	f.repos.stored[id] = wallet.Snapshot{ID: id, PlayerID: player, Balance: balance, Version: 1, CreatedAt: t0, UpdatedAt: t0}
	return id
}

func TestOpen_PositiveWritesInOrder(t *testing.T) {
	f := newFixture(t)

	snap, err := f.svc.Open(t.Context(), wallets.OpenCommand{PlayerID: idgen.V7{}.New(), Initial: brl(t, "1000.00"), CorrelationID: "c-1"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	want := calls{"wallets.Insert", "transactions.Insert", "ledger.Append", "outbox.Append"}
	if !slices.Equal(f.repos.log, want) {
		t.Errorf("writes = %v, want %v", f.repos.log, want)
	}
	if snap.Version != 1 || snap.Balance.String() != "1000.00" {
		t.Errorf("snapshot = %+v, want version 1 and balance 1000.00", snap)
	}
	if len(f.repos.outbox) != 2 || f.repos.outbox[0].EventType != "WagerTransactionProcessed" ||
		f.repos.outbox[1].EventType != "WalletBalanceChanged" {
		t.Errorf("outbox = %+v, want Processed then BalanceChanged", f.repos.outbox)
	}
}

func TestOpen_ZeroBalanceWritesWalletOnly(t *testing.T) {
	f := newFixture(t)

	if _, err := f.svc.Open(t.Context(), wallets.OpenCommand{PlayerID: idgen.V7{}.New(), Initial: brl(t, "0.00"), CorrelationID: "c-1"}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	if !slices.Equal(f.repos.log, calls{"wallets.Insert"}) {
		t.Errorf("writes = %v, want the wallet only", f.repos.log)
	}
}

func TestOpen_AlreadyExists(t *testing.T) {
	f := newFixture(t)
	player := idgen.V7{}.New()
	existing := storeWallet(t, f, player, "BRL", "50.00")
	f.repos.insertWalletErr = fmt.Errorf("fake: %w", app.ErrAlreadyExists)

	_, err := f.svc.Open(t.Context(), wallets.OpenCommand{PlayerID: player, Initial: brl(t, "0.00"), CorrelationID: "c-1"})

	if !errors.Is(err, wallets.ErrWalletAlreadyExists) {
		t.Errorf("err = %v, want ErrWalletAlreadyExists", err)
	}
	if !errors.Is(err, app.ErrAlreadyExists) {
		t.Errorf("err = %v, want the repository's ErrAlreadyExists kept in the chain", err)
	}
	var exists *wallets.AlreadyExistsError
	if !errors.As(err, &exists) || exists.WalletID != existing {
		t.Errorf("err = %v, want an AlreadyExistsError naming wallet %s", err, existing)
	}
	if f.uow.doCalls != 1 {
		t.Errorf("units of work = %d, want 1: a duplicate is final, not retried", f.uow.doCalls)
	}
	if f.uow.snapshots != 1 {
		t.Errorf("snapshots = %d, want 1 to find the existing wallet", f.uow.snapshots)
	}
}

func TestOpen_AlreadyExistsNamesTheWalletOfThatCurrency(t *testing.T) {
	f := newFixture(t)
	player := idgen.V7{}.New()
	usd := storeWallet(t, f, player, "USD", "0.00")
	brlWallet := storeWallet(t, f, player, "BRL", "50.00")
	f.repos.insertWalletErr = fmt.Errorf("fake: %w", app.ErrAlreadyExists)

	_, err := f.svc.Open(t.Context(), wallets.OpenCommand{PlayerID: player, Initial: brl(t, "0.00"), CorrelationID: "c-1"})

	var exists *wallets.AlreadyExistsError
	if !errors.As(err, &exists) || exists.WalletID != brlWallet || exists.WalletID == usd {
		t.Errorf("err = %v, want the BRL wallet %s, not the USD one %s", err, brlWallet, usd)
	}
}

func TestOpen_AlreadyExistsLookupFailure(t *testing.T) {
	f := newFixture(t)
	f.repos.insertWalletErr = fmt.Errorf("fake: %w", app.ErrAlreadyExists)
	f.repos.lookupErr = fmt.Errorf("fake: %w", app.ErrUnavailable)

	_, err := f.svc.Open(t.Context(), wallets.OpenCommand{PlayerID: idgen.V7{}.New(), Initial: brl(t, "0.00"), CorrelationID: "c-1"})

	if !errors.Is(err, app.ErrUnavailable) || errors.Is(err, wallets.ErrWalletAlreadyExists) {
		t.Errorf("err = %v, want ErrUnavailable and not ErrWalletAlreadyExists", err)
	}
}

func TestOpen_ContentionExhaustedIsUnavailable(t *testing.T) {
	f := newFixture(t)
	f.repos.insertWalletErr = fmt.Errorf("fake: %w", app.Retryable(app.RetryLockTimeout, errors.New("lock timeout")))

	_, err := f.svc.Open(t.Context(), wallets.OpenCommand{PlayerID: idgen.V7{}.New(), Initial: brl(t, "0.00"), CorrelationID: "c-1"})

	if !errors.Is(err, app.ErrUnavailable) || !errors.Is(err, app.ErrRetryable) {
		t.Errorf("err = %v, want ErrUnavailable wrapping the last ErrRetryable", err)
	}
	if f.uow.doCalls != txPolicy.Attempts {
		t.Errorf("units of work = %d, want %d", f.uow.doCalls, txPolicy.Attempts)
	}
	if want := []string{app.RetryLockTimeout, app.RetryLockTimeout}; !slices.Equal(f.metrics.retries, want) {
		t.Errorf("TxRetry calls = %v, want %v", f.metrics.retries, want)
	}
}

func TestOpen_RetrySucceeds(t *testing.T) {
	f := newFixture(t)
	failures := 1
	f.uow.repos.insertWalletErr = nil
	uow := &flakyUoW{fakeUoW: f.uow, failures: &failures}
	svc := wallets.NewService(uow, &clocktest.Fixed{T: t0}, idgen.V7{}, f.metrics, slog.New(slog.DiscardHandler), txPolicy)

	if _, err := svc.Open(t.Context(), wallets.OpenCommand{PlayerID: idgen.V7{}.New(), Initial: brl(t, "0.00"), CorrelationID: "c-1"}); err != nil {
		t.Errorf("Open after one retryable failure: %v", err)
	}
	if f.uow.doCalls != 2 {
		t.Errorf("units of work = %d, want 2", f.uow.doCalls)
	}
	if want := []string{app.RetrySerialization}; !slices.Equal(f.metrics.retries, want) {
		t.Errorf("TxRetry calls = %v, want %v", f.metrics.retries, want)
	}
}

type flakyUoW struct {
	*fakeUoW
	failures *int
}

func (u *flakyUoW) Do(ctx context.Context, fn func(context.Context, app.Repos) error) error {
	if *u.failures > 0 {
		*u.failures--
		u.doCalls++
		return fmt.Errorf("fake: %w", app.Retryable(app.RetrySerialization, errors.New("serialization failure")))
	}
	return u.fakeUoW.Do(ctx, fn)
}

func TestGet_NotFound(t *testing.T) {
	f := newFixture(t)

	_, err := f.svc.Get(t.Context(), idgen.V7{}.New())

	if !errors.Is(err, wallets.ErrWalletNotFound) {
		t.Errorf("err = %v, want ErrWalletNotFound", err)
	}
}

func openedWallet(t *testing.T, f *fixture, balance string) uuid.UUID {
	t.Helper()
	snap, err := f.svc.Open(t.Context(), wallets.OpenCommand{PlayerID: idgen.V7{}.New(), Initial: brl(t, balance), CorrelationID: "c-1"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	f.repos.log = nil
	return snap.ID
}

func TestLedger_NextAfterSeq(t *testing.T) {
	f := newFixture(t)
	id := openedWallet(t, f, "0.00")
	for _, seq := range []int64{3, 7, 9} {
		f.repos.page = append(f.repos.page, app.LedgerRecord{Seq: seq})
	}

	first, err := f.svc.Ledger(t.Context(), id, 0, 2)
	if err != nil {
		t.Fatalf("Ledger: %v", err)
	}
	if len(first.Records) != 2 || first.NextAfterSeq == nil || *first.NextAfterSeq != 7 {
		t.Fatalf("first page = %d records, next %v; want 2 and 7", len(first.Records), first.NextAfterSeq)
	}
	last, err := f.svc.Ledger(t.Context(), id, *first.NextAfterSeq, 2)
	if err != nil {
		t.Fatalf("Ledger: %v", err)
	}
	if len(last.Records) != 1 || last.NextAfterSeq != nil {
		t.Errorf("last page = %d records, next %v; want 1 and nil", len(last.Records), last.NextAfterSeq)
	}
	exact, err := f.svc.Ledger(t.Context(), id, 0, 3)
	if err != nil {
		t.Fatalf("Ledger: %v", err)
	}
	if len(exact.Records) != 3 || exact.NextAfterSeq != nil {
		t.Errorf("page that ends exactly at the last entry = %d records, next %v; want 3 and nil", len(exact.Records), exact.NextAfterSeq)
	}
}

func TestLedger_InvalidQuery(t *testing.T) {
	f := newFixture(t)
	id := openedWallet(t, f, "0.00")

	for _, q := range []struct {
		after int64
		limit int
	}{{0, 0}, {0, -1}, {-1, 10}} {
		if _, err := f.svc.Ledger(t.Context(), id, q.after, q.limit); !errors.Is(err, wallets.ErrInvalidLedgerQuery) {
			t.Errorf("Ledger(after %d, limit %d) err = %v, want ErrInvalidLedgerQuery", q.after, q.limit, err)
		}
	}
	if _, err := f.svc.Ledger(t.Context(), idgen.V7{}.New(), 0, 10); !errors.Is(err, wallets.ErrWalletNotFound) {
		t.Errorf("ledger of an unknown wallet: err = %v, want ErrWalletNotFound", err)
	}
}

func TestReconcile_DivergenceReportedOnce(t *testing.T) {
	f := newFixture(t)
	id := openedWallet(t, f, "1000.00")
	f.repos.totals = app.LedgerTotals{NetMinor: 100000 - 2500, Count: 2}

	got, err := f.svc.Reconcile(t.Context(), id)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got.Consistent || got.Stored.String() != "1000.00" || got.Calculated.String() != "975.00" ||
		got.Difference.String() != "25.00" || got.CheckedEntries != 2 {
		t.Errorf("reconciliation = %+v, want stored 1000.00, calculated 975.00, difference 25.00, 2 entries", got)
	}
	if f.metrics.divergences != 1 {
		t.Errorf("divergences = %d, want 1", f.metrics.divergences)
	}
	var line map[string]any
	if err := json.Unmarshal(f.logs.Bytes(), &line); err != nil {
		t.Fatalf("log %q is not a single JSON line: %v", f.logs, err)
	}
	want := map[string]any{"level": "ERROR", "walletId": id.String(), "currency": "BRL", "checkedEntries": float64(2)}
	for key, w := range want {
		if line[key] != w {
			t.Errorf("log %s = %v, want %v", key, line[key], w)
		}
	}
	for _, amount := range []string{"1000.00", "975.00", "25.00"} {
		if strings.Contains(f.logs.String(), amount) {
			t.Errorf("log %s holds the amount %s", f.logs, amount)
		}
	}
	if len(f.repos.log) != 0 || f.uow.snapshots == 0 {
		t.Errorf("writes = %v, snapshots = %d; want a read-only unit of work and no write", f.repos.log, f.uow.snapshots)
	}
}

func TestReconcile_ConsistentReportsNothing(t *testing.T) {
	f := newFixture(t)
	id := openedWallet(t, f, "0.00")

	got, err := f.svc.Reconcile(t.Context(), id)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !got.Consistent || !got.Difference.IsZero() || got.CheckedEntries != 0 || f.metrics.divergences != 0 {
		t.Errorf("reconciliation = %+v, divergences %v; want consistent with no entries and no metric", got, f.metrics.divergences)
	}
}
