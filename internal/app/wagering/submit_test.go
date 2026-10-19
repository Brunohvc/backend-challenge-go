package wagering_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/clock/clocktest"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

var submittedAt = time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)

const inboxConsumer = "wager-transactions"

var viaHTTP = wagering.Source{Channel: wagering.ChannelHTTP, CorrelationID: "corr-1"}

func viaQueue(messageID, hash string) wagering.Source {
	return wagering.Source{
		Channel:       wagering.ChannelSQS,
		CorrelationID: "corr-" + messageID,
		CausationID:   messageID,
		Inbox:         &wagering.InboxMessage{Consumer: inboxConsumer, MessageID: messageID, Hash: hash},
	}
}

var settledWrites = []string{"transactions.Insert", "ledger.Append", "wallets.UpdateBalance", "outbox.Append"}

type inboxKey struct{ consumer, messageID string }

type inboxEntry struct {
	hash    string
	txID    uuid.NullUUID
	outcome string
}

type fakeState struct {
	wallets map[uuid.UUID]wallet.Snapshot
	txs     map[uuid.UUID]wager.Snapshot
	inbox   map[inboxKey]inboxEntry
	writes  []string
}

func (s fakeState) clone() fakeState {
	return fakeState{
		wallets: maps.Clone(s.wallets),
		txs:     maps.Clone(s.txs),
		inbox:   maps.Clone(s.inbox),
		writes:  slices.Clone(s.writes),
	}
}

type fakeUoW struct {
	repos   *fakeRepos
	doCalls int
}

func (u *fakeUoW) Do(ctx context.Context, fn func(context.Context, app.Repos) error) error {
	u.doCalls++
	before := u.repos.state.clone()
	if err := fn(ctx, u.repos); err != nil {
		u.repos.state = before
		return err
	}
	return nil
}

func (u *fakeUoW) Snapshot(ctx context.Context, fn func(context.Context, app.Repos) error) error {
	u.repos.readOnly = true
	defer func() { u.repos.readOnly = false }()
	return fn(ctx, u.repos)
}

type fakeRepos struct {
	t        *testing.T
	state    fakeState
	readOnly bool
}

func (r *fakeRepos) write(what string) {
	r.t.Helper()
	if r.readOnly {
		r.t.Errorf("%s inside a read-only unit of work", what)
	}
	r.state.writes = append(r.state.writes, what)
}

func (r *fakeRepos) Wallets() app.WalletRepository           { return fakeWallets{r: r} }
func (r *fakeRepos) Ledger() app.LedgerRepository            { return fakeLedger{r: r} }
func (r *fakeRepos) Transactions() app.TransactionRepository { return fakeTransactions{r: r} }
func (r *fakeRepos) Outbox() app.OutboxRepository            { return fakeOutbox{r: r} }
func (r *fakeRepos) Inbox() app.InboxRepository              { return fakeInbox{r: r} }

type fakeWallets struct {
	app.WalletRepository
	r *fakeRepos
}

func (f fakeWallets) GetForUpdate(_ context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	s, ok := f.r.state.wallets[id]
	if !ok {
		return nil, fmt.Errorf("fake: wallet %s: %w", id, app.ErrNotFound)
	}
	return wallet.Rehydrate(s)
}

func (f fakeWallets) UpdateBalance(_ context.Context, w *wallet.Wallet, _ int64) error {
	f.r.write("wallets.UpdateBalance")
	f.r.state.wallets[w.ID()] = w.Snapshot()
	return nil
}

type fakeLedger struct {
	app.LedgerRepository
	r *fakeRepos
}

func (f fakeLedger) Append(context.Context, wallet.LedgerEntry) error {
	f.r.write("ledger.Append")
	return nil
}

type fakeTransactions struct {
	app.TransactionRepository
	r *fakeRepos
}

func (f fakeTransactions) Insert(_ context.Context, tx *wager.Transaction) error {
	f.r.write("transactions.Insert")
	f.r.state.txs[tx.ID()] = tx.Snapshot()
	return nil
}

func (f fakeTransactions) Get(_ context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return f.find(func(s wager.Snapshot) bool { return s.ID == id })
}

func (f fakeTransactions) FindByIdempotencyKey(_ context.Context, providerID, key string) (*wager.Transaction, error) {
	return f.find(func(s wager.Snapshot) bool { return s.ProviderID == providerID && s.IdempotencyKey == key })
}

func (f fakeTransactions) FindByExternalID(_ context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return f.find(func(s wager.Snapshot) bool {
		return s.ProviderID == providerID && s.ExternalTransactionID == externalID
	})
}

func (f fakeTransactions) find(match func(wager.Snapshot) bool) (*wager.Transaction, error) {
	for _, s := range f.r.state.txs {
		if match(s) {
			return wager.Rehydrate(s)
		}
	}
	return nil, fmt.Errorf("fake: transaction: %w", app.ErrNotFound)
}

type fakeOutbox struct {
	app.OutboxRepository
	r *fakeRepos
}

func (f fakeOutbox) Append(context.Context, ...app.OutboxRecord) error {
	f.r.write("outbox.Append")
	return nil
}

type fakeInbox struct{ r *fakeRepos }

func (f fakeInbox) Claim(_ context.Context, name, messageID, hash string, _ time.Time) (app.InboxClaim, error) {
	key := inboxKey{name, messageID}
	if e, ok := f.r.state.inbox[key]; ok {
		return app.InboxClaim{ExistingHash: e.hash, TransactionID: e.txID}, nil
	}
	f.r.write("inbox.Claim")
	f.r.state.inbox[key] = inboxEntry{hash: hash}
	return app.InboxClaim{New: true}, nil
}

func (f fakeInbox) Complete(_ context.Context, name, messageID string, txID uuid.NullUUID, outcome string, _ time.Time) error {
	f.r.write("inbox.Complete")
	key := inboxKey{name, messageID}
	e := f.r.state.inbox[key]
	e.txID, e.outcome = txID, outcome
	f.r.state.inbox[key] = e
	return nil
}

type duplicateMetrics struct{ duplicates []string }

func (*duplicateMetrics) Transaction(string, string, string, string, time.Duration) {}

func (m *duplicateMetrics) Duplicate(channel, source string) {
	m.duplicates = append(m.duplicates, channel+"/"+source)
}

func (*duplicateMetrics) TxRetry(string) {}

type fixture struct {
	svc      *wagering.Service
	uow      *fakeUoW
	repos    *fakeRepos
	metrics  *duplicateMetrics
	walletID uuid.UUID
	playerID uuid.UUID
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	balance, err := money.Parse("100.00", "BRL")
	if err != nil {
		t.Fatalf("money: %v", err)
	}
	w := wallet.Snapshot{
		ID: idgen.V7{}.New(), PlayerID: idgen.V7{}.New(), Balance: balance, Version: 1,
		CreatedAt: submittedAt, UpdatedAt: submittedAt,
	}
	repos := &fakeRepos{t: t, state: fakeState{
		wallets: map[uuid.UUID]wallet.Snapshot{w.ID: w},
		txs:     map[uuid.UUID]wager.Snapshot{},
		inbox:   map[inboxKey]inboxEntry{},
	}}
	f := &fixture{uow: &fakeUoW{repos: repos}, repos: repos, metrics: &duplicateMetrics{}, walletID: w.ID, playerID: w.PlayerID}
	f.svc = f.service(tracing.Tracer{})
	return f
}

func (f *fixture) service(tr tracing.Tracer) *wagering.Service {
	return wagering.NewService(f.uow, &clocktest.Fixed{T: submittedAt}, idgen.V7{}, f.metrics, tr,
		slog.New(slog.DiscardHandler), wagering.Config{Tx: retry.Policy{Attempts: 3}})
}

func (f *fixture) bet(amount string) wager.CommandInput {
	return wager.CommandInput{
		ProviderID:            "provider-a",
		ExternalTransactionID: "ext-1",
		IdempotencyKey:        "key-1",
		PlayerID:              f.playerID.String(),
		WalletID:              f.walletID.String(),
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  "BET",
		Amount:                amount,
		Currency:              "BRL",
	}
}

func (f *fixture) submit(t *testing.T, in wager.CommandInput, src wagering.Source) (wagering.Result, error) {
	t.Helper()
	cmd, err := wager.NewCommand(in)
	if err != nil {
		t.Fatalf("NewCommand(%+v): %v", in, err)
	}
	return f.svc.Submit(t.Context(), cmd, src)
}

func (f *fixture) settle(t *testing.T, in wager.CommandInput, src wagering.Source) wager.Snapshot {
	t.Helper()
	res, err := f.submit(t, in, src)
	if err != nil {
		t.Fatalf("Submit(%s %s): %v", in.Kind, in.Amount, err)
	}
	if res.Replay || res.DuplicateMessage || res.Transaction.Status != wager.StatusProcessed {
		t.Fatalf("first submission = %+v, want a new PROCESSED transaction", res)
	}
	f.repos.state.writes = nil
	f.metrics.duplicates = nil
	f.uow.doCalls = 0
	return res.Transaction
}

func (f *fixture) requireUntouched(t *testing.T, balance string) {
	t.Helper()
	if len(f.repos.state.writes) != 0 {
		t.Errorf("writes = %v, want none", f.repos.state.writes)
	}
	if got := f.repos.state.wallets[f.walletID].Balance.String(); got != balance {
		t.Errorf("wallet balance = %s, want %s", got, balance)
	}
	if len(f.repos.state.txs) != 1 {
		t.Errorf("%d transactions stored, want the first one only", len(f.repos.state.txs))
	}
}

func TestSubmit_SameKeySamePayloadIsAReplay(t *testing.T) {
	f := newFixture(t)
	in := f.bet("25.00")
	first := f.settle(t, in, viaHTTP)

	res, err := f.submit(t, in, viaHTTP)
	if err != nil {
		t.Fatalf("Submit again: %v", err)
	}

	if !res.Replay || res.DuplicateMessage {
		t.Errorf("result = %+v, want a replay", res)
	}
	if !reflect.DeepEqual(res.Transaction, first) {
		t.Errorf("replay answered %+v, want the stored %+v", res.Transaction, first)
	}
	f.requireUntouched(t, "75.00")
	if f.uow.doCalls != 0 {
		t.Errorf("units of work = %d, want 0: a replay is answered from a read", f.uow.doCalls)
	}
	if want := []string{"http/idempotency"}; !slices.Equal(f.metrics.duplicates, want) {
		t.Errorf("duplicates = %v, want %v", f.metrics.duplicates, want)
	}
}

func TestSubmit_SameKeyOtherPayloadIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*wager.CommandInput)
	}{
		{"another amount", func(in *wager.CommandInput) { in.Amount = "30.00" }},
		{"another round", func(in *wager.CommandInput) { in.RoundID = "round-2" }},
		{"another game", func(in *wager.CommandInput) { in.GameID = "game-2" }},
		{"another external id", func(in *wager.CommandInput) { in.ExternalTransactionID = "ext-2" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			in := f.bet("25.00")
			f.settle(t, in, viaHTTP)
			tc.change(&in)

			res, err := f.submit(t, in, viaHTTP)

			if !errors.Is(err, wagering.ErrIdempotencyKeyReused) {
				t.Fatalf("err = %v, want ErrIdempotencyKeyReused", err)
			}
			if res.Replay || res.Transaction.ID != uuid.Nil {
				t.Errorf("result = %+v, want none", res)
			}
			f.requireUntouched(t, "75.00")
			if f.uow.doCalls != 0 {
				t.Errorf("units of work = %d, want 0: the conflict is found by a read", f.uow.doCalls)
			}
		})
	}
}

func TestSubmit_KnownExternalIDUnderANewKeyIsRefused(t *testing.T) {
	f := newFixture(t)
	in := f.bet("25.00")
	f.settle(t, in, viaHTTP)
	in.IdempotencyKey = "key-2"

	_, err := f.submit(t, in, viaHTTP)

	if !errors.Is(err, wagering.ErrDuplicateExternalTransaction) {
		t.Fatalf("err = %v, want ErrDuplicateExternalTransaction", err)
	}
	f.requireUntouched(t, "75.00")
	if f.uow.doCalls != 1 {
		t.Errorf("units of work = %d, want 1: a duplicate is final, not retried", f.uow.doCalls)
	}
}

func TestSubmit_KeyIsScopedByProvider(t *testing.T) {
	f := newFixture(t)
	in := f.bet("25.00")
	first := f.settle(t, in, viaHTTP)
	in.ProviderID = "provider-b"

	res, err := f.submit(t, in, viaHTTP)
	if err != nil {
		t.Fatalf("Submit under another provider: %v", err)
	}

	tx := res.Transaction
	if res.Replay || tx.ID == first.ID || tx.ProviderID != "provider-b" || tx.Status != wager.StatusProcessed {
		t.Errorf("result = %+v, want a new PROCESSED transaction of provider-b", res)
	}
	if !slices.Equal(f.repos.state.writes, settledWrites) {
		t.Errorf("writes = %v, want %v", f.repos.state.writes, settledWrites)
	}
	if got := f.repos.state.wallets[f.walletID].Balance.String(); got != "50.00" {
		t.Errorf("wallet balance = %s, want 50.00 after both bets", got)
	}
}

func TestSubmit_RedeliveredMessageIsADuplicate(t *testing.T) {
	f := newFixture(t)
	in, src := f.bet("25.00"), viaQueue("msg-1", "hash-1")
	first := f.settle(t, in, src)

	res, err := f.submit(t, in, src)
	if err != nil {
		t.Fatalf("Submit the redelivery: %v", err)
	}

	if !res.DuplicateMessage || res.Replay {
		t.Errorf("result = %+v, want a duplicate message", res)
	}
	if !reflect.DeepEqual(res.Transaction, first) {
		t.Errorf("duplicate answered %+v, want the stored %+v", res.Transaction, first)
	}
	f.requireUntouched(t, "75.00")
	if want := []string{"sqs/inbox"}; !slices.Equal(f.metrics.duplicates, want) {
		t.Errorf("duplicates = %v, want %v", f.metrics.duplicates, want)
	}
}

func TestSubmit_MessageIDReusedForAnotherPayload(t *testing.T) {
	f := newFixture(t)
	in := f.bet("25.00")
	f.settle(t, in, viaQueue("msg-1", "hash-1"))
	other := f.bet("30.00")
	other.ExternalTransactionID, other.IdempotencyKey = "ext-2", "key-2"

	res, err := f.submit(t, other, viaQueue("msg-1", "hash-2"))

	if !errors.Is(err, wagering.ErrMessageIDReused) {
		t.Fatalf("err = %v, want ErrMessageIDReused", err)
	}
	if res.DuplicateMessage || res.Transaction.ID != uuid.Nil {
		t.Errorf("result = %+v, want none", res)
	}
	f.requireUntouched(t, "75.00")
	if e := f.repos.state.inbox[inboxKey{inboxConsumer, "msg-1"}]; e.hash != "hash-1" {
		t.Errorf("inbox holds the hash %q for msg-1, want the first one", e.hash)
	}
}

func TestSubmit_NewMessageWithAStoredKeyIsAReplay(t *testing.T) {
	f := newFixture(t)
	in := f.bet("25.00")
	first := f.settle(t, in, viaHTTP)

	res, err := f.submit(t, in, viaQueue("msg-1", "hash-1"))
	if err != nil {
		t.Fatalf("Submit from the queue: %v", err)
	}

	if !res.Replay || res.DuplicateMessage || !reflect.DeepEqual(res.Transaction, first) {
		t.Errorf("result = %+v, want a replay of %s", res, first.ID)
	}
	if want := []string{"inbox.Claim", "inbox.Complete"}; !slices.Equal(f.repos.state.writes, want) {
		t.Errorf("writes = %v, want %v", f.repos.state.writes, want)
	}
	want := inboxEntry{hash: "hash-1", txID: uuid.NullUUID{UUID: first.ID, Valid: true}, outcome: "REPLAY"}
	if got := f.repos.state.inbox[inboxKey{inboxConsumer, "msg-1"}]; got != want {
		t.Errorf("inbox entry = %+v, want %+v", got, want)
	}
	if want := []string{"sqs/idempotency"}; !slices.Equal(f.metrics.duplicates, want) {
		t.Errorf("duplicates = %v, want %v", f.metrics.duplicates, want)
	}
}

func TestSubmit_NewMessageReusingAKeyIsRefused(t *testing.T) {
	f := newFixture(t)
	in := f.bet("25.00")
	f.settle(t, in, viaHTTP)
	in.Amount = "30.00"

	_, err := f.submit(t, in, viaQueue("msg-1", "hash-1"))

	if !errors.Is(err, wagering.ErrIdempotencyKeyReused) {
		t.Fatalf("err = %v, want ErrIdempotencyKeyReused", err)
	}
	f.requireUntouched(t, "75.00")
	if _, found := f.repos.state.inbox[inboxKey{inboxConsumer, "msg-1"}]; found {
		t.Errorf("msg-1 is in the inbox, want its claim rolled back with the refusal")
	}
}
