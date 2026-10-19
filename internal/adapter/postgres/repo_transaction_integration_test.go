//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

type txRead func(context.Context, app.TransactionRepository) (*wager.Transaction, error)

func txInput(w *wallet.Wallet, kind, amount, ext string) wager.CommandInput {
	return wager.CommandInput{
		ProviderID:            "provider-a",
		ExternalTransactionID: ext,
		IdempotencyKey:        "key-" + ext,
		PlayerID:              w.PlayerID().String(),
		WalletID:              w.ID().String(),
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  kind,
		Amount:                amount,
		Currency:              w.Currency().Code(),
	}
}

func txReferring(in wager.CommandInput, ref string) wager.CommandInput {
	in.ReferenceExternalTransactionID = ref
	return in
}

func txNew(t *testing.T, in wager.CommandInput, at time.Time) *wager.Transaction {
	t.Helper()
	cmd, err := wager.NewCommand(in)
	if err != nil {
		t.Fatalf("NewCommand(%s) error = %v", in.ExternalTransactionID, err)
	}
	tx, err := wager.NewExternal(newID(), cmd, "corr-"+in.ExternalTransactionID, at)
	if err != nil {
		t.Fatalf("NewExternal(%s) error = %v", in.ExternalTransactionID, err)
	}
	return tx
}

func txMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func txRef(tx *wager.Transaction) uuid.NullUUID {
	return uuid.NullUUID{UUID: tx.ID(), Valid: true}
}

func txRefused(t *testing.T, w *wallet.Wallet, in wager.CommandInput, code wager.FailureCode) *wager.Transaction {
	t.Helper()
	tx := txNew(t, in, t0)
	txMust(t, tx.Reject(code, w.Balance(), w.Version(), t1))
	return tx
}

func txRejected(t *testing.T, w *wallet.Wallet, in wager.CommandInput) *wager.Transaction {
	t.Helper()
	return txRefused(t, w, in, wager.FailureInsufficientFunds)
}

func txWaiting(t *testing.T, w *wallet.Wallet, ext string, next time.Time) *wager.Transaction {
	t.Helper()
	created := next.Add(-time.Minute)
	tx := txNew(t, txReferring(txInput(w, "REFUND", "10.00", ext), "bet-not-here-yet"), created)
	txMust(t, tx.AwaitReference(next, next.Add(time.Hour), created))
	return tx
}

func txResolved(t *testing.T, tx, ref *wager.Transaction) *wager.Transaction {
	t.Helper()
	s := tx.Snapshot()
	s.ReferenceTransactionID = txRef(ref)
	resolved, err := wager.Rehydrate(s)
	if err != nil {
		t.Fatalf("Rehydrate() error = %v", err)
	}
	return resolved
}

func txOpening(t *testing.T, w *wallet.Wallet, entry *wallet.LedgerEntry) *wager.Transaction {
	t.Helper()
	opening, err := wager.NewOpening(entry.TransactionID(), w.ID(), w.PlayerID(), entry.Amount(), "corr-opening", t0)
	if err != nil {
		t.Fatalf("NewOpening() error = %v", err)
	}
	txMust(t, opening.MarkProcessed(wager.Result{BalanceAfter: entry.BalanceAfter(), WalletVersion: w.Version()}, t0))
	return opening
}

func txStore(t *testing.T, uow app.UnitOfWork, txs ...*wager.Transaction) {
	t.Helper()
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		for _, tx := range txs {
			if err := r.Transactions().Insert(ctx, tx); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("store transactions: %v", err)
	}
}

func txApply(ctx context.Context, r app.Repos, tx *wager.Transaction, dir wallet.Direction, ref uuid.NullUUID) error {
	w, err := r.Wallets().GetForUpdate(ctx, tx.WalletID())
	if err != nil {
		return err
	}
	read := w.Version()
	move := w.Credit
	if dir == wallet.DirectionDebit {
		move = w.Debit
	}
	entry, err := move(tx.Money(), tx.ID(), newID(), t1)
	if err != nil {
		return err
	}
	result := wager.Result{BalanceAfter: w.Balance(), WalletVersion: w.Version(), ReferenceTransactionID: ref}
	if err := tx.MarkProcessed(result, t1); err != nil {
		return err
	}
	if err := r.Transactions().Insert(ctx, tx); err != nil {
		return err
	}
	if err := r.Ledger().Append(ctx, entry); err != nil {
		return err
	}
	return r.Wallets().UpdateBalance(ctx, w, read)
}

func txProcess(t *testing.T, uow app.UnitOfWork, tx *wager.Transaction, dir wallet.Direction, ref uuid.NullUUID) {
	t.Helper()
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		return txApply(ctx, r, tx, dir, ref)
	})
	if err != nil {
		t.Fatalf("process %s %s: %v", tx.Kind(), tx.ExternalTransactionID(), err)
	}
}

func txLoad(t *testing.T, uow app.UnitOfWork, read txRead) (*wager.Transaction, error) {
	t.Helper()
	var tx *wager.Transaction
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		tx, err = read(ctx, r.Transactions())
		return err
	})
	return tx, err
}

func txGet(t *testing.T, uow app.UnitOfWork, id uuid.UUID) *wager.Transaction {
	t.Helper()
	tx, err := txLoad(t, uow, func(ctx context.Context, r app.TransactionRepository) (*wager.Transaction, error) {
		return r.Get(ctx, id)
	})
	if err != nil {
		t.Fatalf("get transaction %s: %v", id, err)
	}
	return tx
}

func txLookups(want *wager.Transaction) map[string]txRead {
	lookups := map[string]txRead{
		"Get": func(ctx context.Context, r app.TransactionRepository) (*wager.Transaction, error) {
			return r.Get(ctx, want.ID())
		},
		"GetForUpdate": func(ctx context.Context, r app.TransactionRepository) (*wager.Transaction, error) {
			return r.GetForUpdate(ctx, want.ID())
		},
	}
	if want.Origin() == wager.OriginExternal {
		lookups["FindByIdempotencyKey"] = func(ctx context.Context, r app.TransactionRepository) (*wager.Transaction, error) {
			return r.FindByIdempotencyKey(ctx, want.ProviderID(), want.IdempotencyKey())
		}
		lookups["FindByExternalID"] = func(ctx context.Context, r app.TransactionRepository) (*wager.Transaction, error) {
			return r.FindByExternalID(ctx, want.ProviderID(), want.ExternalTransactionID())
		}
	}
	return lookups
}

func txRequireSnapshot(t *testing.T, got, want wager.Snapshot) {
	t.Helper()
	for name, at := range map[string]*time.Time{
		"created at": &got.CreatedAt, "updated at": &got.UpdatedAt, "next attempt at": got.NextAttemptAt,
		"expires at": got.ExpiresAt, "processed at": got.ProcessedAt,
	} {
		if at != nil && at.Location() != time.UTC {
			t.Fatalf("%s is in %s, want UTC", name, at.Location())
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("transaction =\n%s\nwant\n%s", txFormat(got), txFormat(want))
	}
}

func txFormat(s wager.Snapshot) string {
	balance, next, expires, processed := s.ResultBalance, s.NextAttemptAt, s.ExpiresAt, s.ProcessedAt
	s.ResultBalance, s.NextAttemptAt, s.ExpiresAt, s.ProcessedAt = nil, nil, nil, nil
	return fmt.Sprintf("%+v ResultBalance:%v NextAttemptAt:%v ExpiresAt:%v ProcessedAt:%v",
		s, txDeref(balance), txDeref(next), txDeref(expires), txDeref(processed))
}

func txDeref[T any](p *T) any {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func txDatabaseNow(t *testing.T) time.Time {
	t.Helper()
	var now time.Time
	if err := connect(t, pg.AppDSN).QueryRow(t.Context(), "SELECT now()").Scan(&now); err != nil {
		t.Fatalf("read the database clock: %v", err)
	}
	return now.UTC()
}

func txClaim(t *testing.T, uow app.UnitOfWork, lease time.Duration, limit int) []app.DueTransaction {
	t.Helper()
	var claimed []app.DueTransaction
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		claimed, err = r.Transactions().ClaimDue(ctx, lease, limit)
		return err
	})
	if err != nil {
		t.Fatalf("ClaimDue() error = %v", err)
	}
	return claimed
}

func txDue(txs ...*wager.Transaction) []app.DueTransaction {
	due := make([]app.DueTransaction, len(txs))
	for i, tx := range txs {
		due[i] = app.DueTransaction{ID: tx.ID(), WalletID: tx.WalletID()}
	}
	return due
}

func txRequireClaimed(t *testing.T, got []app.DueTransaction, want ...*wager.Transaction) {
	t.Helper()
	claimed := make([]app.DueTransaction, len(got))
	for i, d := range got {
		claimed[i] = app.DueTransaction{ID: d.ID, WalletID: d.WalletID}
	}
	if !slices.Equal(claimed, txDue(want...)) {
		t.Fatalf("claimed %v, want %v", claimed, txDue(want...))
	}
}

func txHold(t *testing.T, uow app.UnitOfWork, fn func(context.Context, app.Repos) error) (release func()) {
	t.Helper()
	held, unhold := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- uow.Do(context.WithoutCancel(t.Context()), func(ctx context.Context, r app.Repos) error {
			if err := fn(ctx, r); err != nil {
				return err
			}
			close(held)
			<-unhold
			return nil
		})
	}()
	select {
	case <-held:
	case err := <-done:
		t.Fatalf("hold a unit of work: %v", err)
	}

	var once sync.Once
	release = func() {
		once.Do(func() {
			close(unhold)
			if err := <-done; err != nil {
				t.Errorf("held unit of work: %v", err)
			}
		})
	}
	t.Cleanup(release)
	return release
}

func txAwaitLockWaiter(t *testing.T, ended <-chan error) {
	t.Helper()
	conn := connect(t, pg.AppDSN)
	deadline := time.Now().Add(10 * time.Second)
	for {
		waiting := queryInt64(t, conn,
			`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`)
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no session is waiting for a lock")
		}
		select {
		case err := <-ended:
			t.Fatalf("the statement ended without waiting for a lock: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestTxRepo_RoundTrip(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)

	w, entry := newWallet(t, newID(), brl(t, 10000))
	opening := txOpening(t, w, entry)
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		if err := r.Wallets().Insert(ctx, w); err != nil {
			return err
		}
		if err := r.Transactions().Insert(ctx, opening); err != nil {
			return err
		}
		return r.Ledger().Append(ctx, *entry)
	})
	if err != nil {
		t.Fatalf("open the wallet: %v", err)
	}

	bet := txNew(t, txInput(w, "BET", "25.00", "bet-1"), t0)
	txProcess(t, uow, bet, wallet.DirectionDebit, uuid.NullUUID{})
	win := txNew(t, txReferring(txInput(w, "WIN", "40.00", "win-1"), "bet-1"), t0)
	txProcess(t, uow, win, wallet.DirectionCredit, txRef(bet))

	current := mustLoadWallet(t, uow, w.ID())
	loss := txNew(t, txInput(w, "LOSS", "0.00", "loss-1"), t0)
	txMust(t, loss.MarkProcessed(wager.Result{BalanceAfter: current.Balance(), WalletVersion: current.Version()}, t1))
	broke := txRejected(t, current, txInput(w, "BET", "500.00", "bet-2"))
	foreign := txNew(t, txInput(w, "BET", "1.00", "bet-3"), t0)
	txMust(t, foreign.Reject(wager.FailureWalletPlayerMismatch, money.Money{}, 0, t1))
	waiting := txNew(t, txReferring(txInput(w, "ROLLBACK", "10.00", "rollback-1"), "bet-later"), t0)
	txMust(t, waiting.AwaitReference(t0.Add(time.Second), t0.Add(time.Hour), t0))
	txMust(t, waiting.RescheduleReference(t0.Add(time.Minute), t0.Add(2*time.Second)))
	txMust(t, waiting.NotePermanentFailure(3, t0.Add(3*time.Second)))
	failed := txNew(t, txReferring(txInput(w, "REFUND", "25.00", "refund-1"), "bet-1"), t0)
	txMust(t, failed.NotePermanentFailure(1, t1))
	txStore(t, uow, loss, broke, foreign, waiting, failed)

	for name, want := range map[string]*wager.Transaction{
		"processed opening":          opening,
		"processed bet":              bet,
		"processed win, resolved":    win,
		"processed loss":             loss,
		"rejected with a balance":    broke,
		"rejected without a balance": foreign,
		"waiting, retried":           waiting,
		"failed":                     failed,
	} {
		for lookup, read := range txLookups(want) {
			t.Run(name+"/"+lookup, func(t *testing.T) {
				got, err := txLoad(t, uow, read)
				if err != nil {
					t.Fatalf("%s() error = %v", lookup, err)
				}
				txRequireSnapshot(t, got.Snapshot(), want.Snapshot())
			})
		}
	}
}

func TestTxRepo_UniqueKeyRace(t *testing.T) {
	for _, tc := range []struct {
		name, constraint string
		twin             func(wager.CommandInput) wager.CommandInput
	}{
		{"same idempotency key", "ux_tx_provider_key", func(in wager.CommandInput) wager.CommandInput {
			in.ExternalTransactionID = "bet-2"
			return in
		}},
		{"same external transaction id", "ux_tx_provider_external", func(in wager.CommandInput) wager.CommandInput {
			in.IdempotencyKey = "another-key"
			return in
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetDB(t)
			uow, _ := newUnitOfWork(t)
			w, _ := openWallet(t, uow, 0)
			in := txInput(w, "BET", "25.00", "bet-1")
			first, second := txRejected(t, w, in), txRejected(t, w, tc.twin(in))

			release := txHold(t, uow, func(ctx context.Context, r app.Repos) error {
				return r.Transactions().Insert(ctx, first)
			})
			ended := make(chan error, 1)
			go func() {
				ended <- uow.Do(context.WithoutCancel(t.Context()), func(ctx context.Context, r app.Repos) error {
					return r.Transactions().Insert(ctx, second)
				})
			}()
			txAwaitLockWaiter(t, ended)
			release()

			err := <-ended
			requireClass(t, err, app.ErrRetryable)
			requireSQLState(t, err, "23505", tc.constraint)
			for lookup, read := range txLookups(first) {
				got, err := txLoad(t, uow, read)
				if err != nil {
					t.Fatalf("%s() error = %v", lookup, err)
				}
				txRequireSnapshot(t, got.Snapshot(), first.Snapshot())
			}
			if n := queryInt64(t, connect(t, pg.AppDSN), "SELECT count(*) FROM wager_transactions"); n != 1 {
				t.Fatalf("%d transactions stored, want only the first one", n)
			}
		})
	}
}

func TestTxRepo_SecondReversalIsRetryable(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 10000)
	bet := txNew(t, txInput(w, "BET", "25.00", "bet-1"), t0)
	txProcess(t, uow, bet, wallet.DirectionDebit, uuid.NullUUID{})
	refund := txNew(t, txReferring(txInput(w, "REFUND", "25.00", "refund-1"), "bet-1"), t0)
	txProcess(t, uow, refund, wallet.DirectionCredit, txRef(bet))

	rollback := txNew(t, txReferring(txInput(w, "ROLLBACK", "25.00", "rollback-1"), "bet-1"), t0)
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		return txApply(ctx, r, rollback, wallet.DirectionCredit, txRef(bet))
	})
	requireClass(t, err, app.ErrRetryable)
	requireSQLState(t, err, "23505", "ux_tx_single_reversal")
}

func TestTxRepo_InsertRefusesPending(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)
	pending := txNew(t, txInput(w, "BET", "25.00", "bet-1"), t0)

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		return r.Transactions().Insert(ctx, pending)
	})
	requireClass(t, err, app.ErrPermanent)
	requireSQLState(t, err, "23514", "ck_pending_schedule")
}

func TestTxRepo_FindByExternalScopedByProvider(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)

	inA := txInput(w, "BET", "25.00", "shared-1")
	inB := inA
	inB.ProviderID = "provider-b"
	onlyB := txInput(w, "BET", "25.00", "only-b")
	onlyB.ProviderID = "provider-b"
	a, b, c := txRejected(t, w, inA), txRejected(t, w, inB), txRejected(t, w, onlyB)
	txStore(t, uow, a, b, c)

	for _, want := range []*wager.Transaction{a, b, c} {
		for lookup, read := range txLookups(want) {
			got, err := txLoad(t, uow, read)
			if err != nil {
				t.Fatalf("%s(%s, %s) error = %v", lookup, want.ProviderID(), want.ExternalTransactionID(), err)
			}
			txRequireSnapshot(t, got.Snapshot(), want.Snapshot())
		}
	}

	for _, tc := range []struct{ name, provider, ext, key string }{
		{"another provider's external id", "provider-a", "only-b", "key-only-b"},
		{"a provider that sent nothing", "provider-c", "shared-1", "key-shared-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := txLoad(t, uow, func(ctx context.Context, r app.TransactionRepository) (*wager.Transaction, error) {
				return r.FindByExternalID(ctx, tc.provider, tc.ext)
			})
			requireClass(t, err, app.ErrNotFound)
			_, err = txLoad(t, uow, func(ctx context.Context, r app.TransactionRepository) (*wager.Transaction, error) {
				return r.FindByIdempotencyKey(ctx, tc.provider, tc.key)
			})
			requireClass(t, err, app.ErrNotFound)
		})
	}
}

func TestTxRepo_HasProcessedReversal(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 10000)
	reversed := func(target uuid.UUID) bool {
		t.Helper()
		var got bool
		err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
			var err error
			got, err = r.Transactions().HasProcessedReversal(ctx, target)
			return err
		})
		if err != nil {
			t.Fatalf("HasProcessedReversal(%s) error = %v", target, err)
		}
		return got
	}

	bet := txNew(t, txInput(w, "BET", "25.00", "bet-1"), t0)
	txProcess(t, uow, bet, wallet.DirectionDebit, uuid.NullUUID{})
	other := txNew(t, txInput(w, "BET", "10.00", "bet-2"), t0)
	txProcess(t, uow, other, wallet.DirectionDebit, uuid.NullUUID{})
	if reversed(bet.ID()) {
		t.Fatal("a bet nothing refers to is reversed")
	}

	win := txNew(t, txReferring(txInput(w, "WIN", "40.00", "win-1"), "bet-1"), t0)
	txProcess(t, uow, win, wallet.DirectionCredit, txRef(bet))
	if reversed(bet.ID()) {
		t.Fatal("the WIN of a bet reverses it")
	}

	refused := txRefused(t, mustLoadWallet(t, uow, w.ID()),
		txReferring(txInput(w, "REFUND", "30.00", "refund-1"), "bet-1"), wager.FailureReversalAmountMismatch)
	txStore(t, uow, txResolved(t, refused, bet))
	if reversed(bet.ID()) {
		t.Fatal("a refused REFUND reverses the bet it names")
	}

	refund := txNew(t, txReferring(txInput(w, "REFUND", "25.00", "refund-2"), "bet-1"), t0)
	txProcess(t, uow, refund, wallet.DirectionCredit, txRef(bet))
	if !reversed(bet.ID()) {
		t.Fatal("a bet with a processed REFUND is not reversed")
	}
	if reversed(other.ID()) {
		t.Fatal("the REFUND of one bet reverses another")
	}

	rollback := txNew(t, txReferring(txInput(w, "ROLLBACK", "10.00", "rollback-1"), "bet-2"), t0)
	txProcess(t, uow, rollback, wallet.DirectionCredit, txRef(other))
	if !reversed(other.ID()) {
		t.Fatal("a bet with a processed ROLLBACK is not reversed")
	}
	if reversed(newID()) {
		t.Fatal("an unknown transaction is reversed")
	}
}

func TestTxRepo_ClaimDue_SkipLocked(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)
	now := txDatabaseNow(t)
	waiting := make([]*wager.Transaction, 10)
	for i := range waiting {
		next := now.Add(-time.Duration(len(waiting)-i) * time.Second)
		waiting[i] = txWaiting(t, w, fmt.Sprintf("refund-%d", i), next)
	}
	txStore(t, uow, append(slices.Clone(waiting[5:]), waiting[:5]...)...)

	var first []app.DueTransaction
	release := txHold(t, uow, func(ctx context.Context, r app.Repos) error {
		var err error
		first, err = r.Transactions().ClaimDue(ctx, time.Minute, 4)
		return err
	})
	second := txClaim(t, uow, time.Minute, len(waiting)-len(first))
	release()

	txRequireClaimed(t, first, waiting[:4]...)
	txRequireClaimed(t, second, waiting[4:]...)
	if again := txClaim(t, uow, time.Minute, len(waiting)); len(again) != 0 {
		t.Fatalf("claimed %v again while their leases run", again)
	}
}

func TestTxRepo_ClaimDue_MostOverdueFirst(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)
	now := txDatabaseNow(t)
	waiting := make([]*wager.Transaction, 12)
	for i := range waiting {
		waiting[i] = txWaiting(t, w, fmt.Sprintf("refund-%d", i), now.Add(-time.Duration(len(waiting)-i)*time.Second))
	}
	stored := slices.Clone(waiting)
	slices.Reverse(stored)
	txStore(t, uow, stored...)

	var claimed []app.DueTransaction
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		if _, err := postgres.TxOf(r).Exec(ctx, `SELECT set_config('enable_indexscan', 'off', true),
		        set_config('enable_indexonlyscan', 'off', true), set_config('enable_bitmapscan', 'off', true)`); err != nil {
			return err
		}
		var err error
		claimed, err = r.Transactions().ClaimDue(ctx, time.Minute, 5)
		return err
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	txRequireClaimed(t, claimed, waiting[:5]...)
}

func TestTxRepo_ClaimDue_TakesReferencedRows(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)
	waiting := txWaiting(t, w, "refund-1", txDatabaseNow(t).Add(-time.Second))
	txStore(t, uow, waiting)

	rollback := txResolved(t, txRefused(t, w, txReferring(txInput(w, "ROLLBACK", "10.00", "rollback-1"), "refund-1"),
		wager.FailureReversalInsufficientFunds), waiting)
	release := txHold(t, uow, func(ctx context.Context, r app.Repos) error {
		return r.Transactions().Insert(ctx, rollback)
	})
	txRequireClaimed(t, txClaim(t, uow, time.Minute, 10), waiting)
	release()
}

func TestTxRepo_ClaimDue_ConcurrentClaimersShareNothing(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)
	now := txDatabaseNow(t)
	waiting := make([]*wager.Transaction, 40)
	for i := range waiting {
		waiting[i] = txWaiting(t, w, fmt.Sprintf("refund-%d", i), now.Add(-time.Minute))
	}
	txStore(t, uow, waiting...)

	var (
		mu      sync.Mutex
		claimed = map[uuid.UUID]int{}
		wg      sync.WaitGroup
		start   = make(chan struct{})
		errs    = make(chan error, 8)
	)
	for range cap(errs) {
		wg.Go(func() {
			<-start
			for {
				var batch []app.DueTransaction
				err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
					var err error
					batch, err = r.Transactions().ClaimDue(ctx, time.Minute, 3)
					return err
				})
				if err != nil {
					errs <- err
					return
				}
				if len(batch) == 0 {
					return
				}
				mu.Lock()
				for _, d := range batch {
					claimed[d.ID]++
				}
				mu.Unlock()
			}
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("ClaimDue() error = %v", err)
	}

	for _, tx := range waiting {
		if n := claimed[tx.ID()]; n != 1 {
			t.Errorf("transaction %s claimed %d times, want once", tx.ID(), n)
		}
	}
	if len(claimed) != len(waiting) {
		t.Errorf("claimed %d transactions, want %d", len(claimed), len(waiting))
	}
}

func TestTxRepo_ClaimDue_UsesDatabaseNow(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)
	now := txDatabaseNow(t)
	due := txWaiting(t, w, "refund-due", now.Add(-time.Second))
	later := txWaiting(t, w, "refund-later", now.Add(time.Hour))
	s := txRejected(t, w, txInput(w, "BET", "1.00", "bet-final")).Snapshot()
	past := now.Add(-time.Hour)
	s.NextAttemptAt = &past
	final, err := wager.Rehydrate(s)
	if err != nil {
		t.Fatalf("Rehydrate() error = %v", err)
	}
	txStore(t, uow, due, later, final)

	const lease = 30 * time.Second
	var (
		claimed []app.DueTransaction
		exact   bool
	)
	err = uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		if claimed, err = r.Transactions().ClaimDue(ctx, lease, 10); err != nil {
			return err
		}
		return postgres.TxOf(r).QueryRow(ctx,
			`SELECT next_attempt_at = now() + make_interval(secs => 30) FROM wager_transactions WHERE id = $1`,
			due.ID()).Scan(&exact)
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	txRequireClaimed(t, claimed, due)
	if !exact {
		t.Fatal("the lease does not end lease after the database's now()")
	}

	got := txGet(t, uow, due.ID()).Snapshot()
	want := due.Snapshot()
	if got.NextAttemptAt == nil || !got.NextAttemptAt.After(now) {
		t.Fatalf("next attempt at = %v, want it moved past %v", txDeref(got.NextAttemptAt), now)
	}
	want.NextAttemptAt = got.NextAttemptAt
	txRequireSnapshot(t, got, want)
	txRequireSnapshot(t, txGet(t, uow, later.ID()).Snapshot(), later.Snapshot())
	txRequireSnapshot(t, txGet(t, uow, final.ID()).Snapshot(), final.Snapshot())
}

func TestTxRepo_ClaimDue_ReturnsTheLease(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)
	now := txDatabaseNow(t)
	first := txWaiting(t, w, "refund-1", now.Add(-2*time.Second))
	second := txWaiting(t, w, "refund-2", now.Add(-time.Second))
	txStore(t, uow, first, second)

	claimed := txClaim(t, uow, time.Minute, 10)

	txRequireClaimed(t, claimed, first, second)
	for _, d := range claimed {
		if d.LeaseUntil.Location() != time.UTC {
			t.Errorf("transaction %s: the lease end is in %s, want UTC", d.ID, d.LeaseUntil.Location())
		}
		if !d.LeaseUntil.After(now) {
			t.Errorf("transaction %s: the lease ends at %v, want it after %v", d.ID, d.LeaseUntil, now)
		}
		stored := txGet(t, uow, d.ID).NextAttemptAt()
		if stored == nil || !d.LeaseUntil.Equal(*stored) {
			t.Errorf("transaction %s: leased until %v, but its next attempt is stored at %v", d.ID, d.LeaseUntil, txDeref(stored))
		}
	}
}

func TestTxRepo_ClaimDue_LeaseExpires(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)
	waiting := txWaiting(t, w, "refund-1", txDatabaseNow(t).Add(-time.Second))
	txStore(t, uow, waiting)

	const lease = 500 * time.Millisecond
	txRequireClaimed(t, txClaim(t, uow, lease, 10), waiting)
	if again := txClaim(t, uow, lease, 10); len(again) != 0 {
		t.Fatalf("claimed %v again while its lease runs", again)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if again := txClaim(t, uow, lease, 10); len(again) > 0 {
			txRequireClaimed(t, again, waiting)
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the transaction was not claimed again after its lease ended")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestTxRepo_ClaimDue_RefusesBadArguments(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)
	waiting := txWaiting(t, w, "refund-1", txDatabaseNow(t).Add(-time.Second))
	txStore(t, uow, waiting)

	for _, tc := range []struct {
		name  string
		lease time.Duration
		limit int
	}{
		{"zero lease", 0, 10},
		{"negative lease", -time.Second, 10},
		{"zero limit", time.Minute, 0},
		{"negative limit", time.Minute, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
				_, err := r.Transactions().ClaimDue(ctx, tc.lease, tc.limit)
				return err
			})
			requireClass(t, err, app.ErrPermanent)
		})
	}
	txRequireClaimed(t, txClaim(t, uow, time.Minute, 10), waiting)
}

func TestTxRepo_GetForUpdateLocksTheRow(t *testing.T) {
	resetDB(t)
	cfg := dbConfig()
	cfg.LockTimeout = 200 * time.Millisecond
	uow := postgres.NewUnitOfWork(newPool(t, cfg), tracing.Tracer{})
	w, _ := openWallet(t, uow, 0)
	waiting := txWaiting(t, w, "refund-1", txDatabaseNow(t).Add(-time.Second))
	txStore(t, uow, waiting)
	lock := func(ctx context.Context, r app.TransactionRepository) (*wager.Transaction, error) {
		return r.GetForUpdate(ctx, waiting.ID())
	}

	release := txHold(t, uow, func(ctx context.Context, r app.Repos) error {
		_, err := lock(ctx, r.Transactions())
		return err
	})

	_, err := txLoad(t, uow, lock)
	requireClass(t, err, app.ErrRetryable)
	requireSQLState(t, err, "55P03", "")
	if claimed := txClaim(t, uow, time.Minute, 10); len(claimed) != 0 {
		t.Fatalf("claimed %v while a writer holds it", claimed)
	}
	txRequireSnapshot(t, txGet(t, uow, waiting.ID()).Snapshot(), waiting.Snapshot())
	rollback := txRefused(t, w, txReferring(txInput(w, "ROLLBACK", "10.00", "rollback-1"), "refund-1"),
		wager.FailureReversalInsufficientFunds)
	txStore(t, uow, txResolved(t, rollback, waiting))

	release()
	txRequireClaimed(t, txClaim(t, uow, time.Minute, 10), waiting)
}

func TestTxRepo_UpdateStoresTransitions(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 10000)
	refund := txNew(t, txReferring(txInput(w, "REFUND", "25.00", "refund-1"), "bet-1"), t0)
	txMust(t, refund.AwaitReference(t0.Add(time.Second), t0.Add(time.Hour), t0))
	txStore(t, uow, refund)

	var retried *wager.Transaction
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		if retried, err = r.Transactions().GetForUpdate(ctx, refund.ID()); err != nil {
			return err
		}
		if err := retried.RescheduleReference(t0.Add(time.Minute), t0.Add(2*time.Second)); err != nil {
			return err
		}
		if err := retried.NotePermanentFailure(3, t0.Add(3*time.Second)); err != nil {
			return err
		}
		return r.Transactions().Update(ctx, retried)
	})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	txRequireSnapshot(t, txGet(t, uow, refund.ID()).Snapshot(), retried.Snapshot())

	bet := txNew(t, txInput(w, "BET", "25.00", "bet-1"), t0)
	txProcess(t, uow, bet, wallet.DirectionDebit, uuid.NullUUID{})

	var processed *wager.Transaction
	err = uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		wl, err := r.Wallets().GetForUpdate(ctx, w.ID())
		if err != nil {
			return err
		}
		read := wl.Version()
		if processed, err = r.Transactions().GetForUpdate(ctx, refund.ID()); err != nil {
			return err
		}
		entry, err := wl.Credit(processed.Money(), processed.ID(), newID(), t1)
		if err != nil {
			return err
		}
		result := wager.Result{BalanceAfter: wl.Balance(), WalletVersion: wl.Version(), ReferenceTransactionID: txRef(bet)}
		if err := processed.MarkProcessed(result, t1); err != nil {
			return err
		}
		if err := r.Transactions().Update(ctx, processed); err != nil {
			return err
		}
		if err := r.Ledger().Append(ctx, entry); err != nil {
			return err
		}
		return r.Wallets().UpdateBalance(ctx, wl, read)
	})
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	stored := txGet(t, uow, refund.ID())
	txRequireSnapshot(t, stored.Snapshot(), processed.Snapshot())

	err = uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		return r.Transactions().Update(ctx, stored)
	})
	requireClass(t, err, app.ErrPermanent)
	requireSQLState(t, err, "WL002", "wager_transactions_guard")
}

func TestTxRepo_UpdateStoresFinalOutcomes(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 10000)
	expired := txWaiting(t, w, "refund-1", t1)
	givenUp := txWaiting(t, w, "refund-2", t1)
	txStore(t, uow, expired, givenUp)

	var updated []*wager.Transaction
	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		wl, err := r.Wallets().GetForUpdate(ctx, w.ID())
		if err != nil {
			return err
		}
		end := map[uuid.UUID]func(*wager.Transaction) error{
			expired.ID(): func(tx *wager.Transaction) error {
				return tx.Reject(wager.FailureReferenceNotFound, wl.Balance(), wl.Version(), t1.Add(time.Hour))
			},
			givenUp.ID(): func(tx *wager.Transaction) error { return tx.NotePermanentFailure(1, t1.Add(time.Second)) },
		}
		for _, id := range []uuid.UUID{expired.ID(), givenUp.ID()} {
			tx, err := r.Transactions().GetForUpdate(ctx, id)
			if err != nil {
				return err
			}
			if err := end[id](tx); err != nil {
				return err
			}
			if err := r.Transactions().Update(ctx, tx); err != nil {
				return err
			}
			updated = append(updated, tx)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("end the waiting transactions: %v", err)
	}
	for _, want := range updated {
		got := txGet(t, uow, want.ID())
		if !got.Status().IsTerminal() {
			t.Fatalf("transaction %s is %s, want it final", got.ID(), got.Status())
		}
		txRequireSnapshot(t, got.Snapshot(), want.Snapshot())
	}
}

func TestTxRepo_UpdateWritesOnlyOutcomeAndSchedule(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)
	waiting := txWaiting(t, w, "refund-1", t1)
	txStore(t, uow, waiting)

	s := waiting.Snapshot()
	s.Money = brl(t, 99999)
	s.RoundID, s.GameID, s.CorrelationID = "round-2", "game-2", "corr-other"
	s.ReferenceAttempts = 1
	edited, err := wager.Rehydrate(s)
	if err != nil {
		t.Fatalf("Rehydrate() error = %v", err)
	}
	err = uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		return r.Transactions().Update(ctx, edited)
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	want := waiting.Snapshot()
	want.ReferenceAttempts = 1
	txRequireSnapshot(t, txGet(t, uow, waiting.ID()).Snapshot(), want)
}

func TestTxRepo_UnknownIsNotFound(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)
	never := txRejected(t, w, txInput(w, "BET", "25.00", "bet-1"))

	for lookup, read := range txLookups(never) {
		t.Run(lookup, func(t *testing.T) {
			_, err := txLoad(t, uow, read)
			requireClass(t, err, app.ErrNotFound)
		})
	}
	t.Run("Update", func(t *testing.T) {
		err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
			return r.Transactions().Update(ctx, never)
		})
		requireClass(t, err, app.ErrNotFound)
	})
}

func TestTxRepo_UnloadableRowIsPermanent(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w := seedWallet(t, connect(t, pg.AppDSN), 0)
	gbp := externalRow(w, "BET", 100).
		set("currency", "GBP").
		set("status", "REJECTED").
		set("failure_code", "CURRENCY_MISMATCH").
		set("result_balance_minor", nil).
		set("result_wallet_version", nil)
	mustInsert(t, connect(t, pg.AppDSN), "wager_transactions", gbp)

	for lookup, read := range map[string]txRead{
		"Get": func(ctx context.Context, r app.TransactionRepository) (*wager.Transaction, error) {
			return r.Get(ctx, get[uuid.UUID](gbp, "id"))
		},
		"FindByExternalID": func(ctx context.Context, r app.TransactionRepository) (*wager.Transaction, error) {
			return r.FindByExternalID(ctx, "provider-a", get[string](gbp, "external_transaction_id"))
		},
	} {
		_, err := txLoad(t, uow, read)
		requireClass(t, err, app.ErrPermanent)
		if !errors.Is(err, money.ErrUnsupportedCurrency) {
			t.Fatalf("%s() error = %v, want it to say why the row does not load", lookup, err)
		}
	}
}

func TestTxRepo_ErrorsAreClassified(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 0)
	tx := txRejected(t, w, txInput(w, "BET", "25.00", "bet-1"))

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		txs := r.Transactions()
		for name, call := range map[string]func() error{
			"Insert": func() error { return txs.Insert(canceled, tx) },
			"Update": func() error { return txs.Update(canceled, tx) },
			"Get": func() error {
				_, err := txs.Get(canceled, tx.ID())
				return err
			},
			"GetForUpdate": func() error {
				_, err := txs.GetForUpdate(canceled, tx.ID())
				return err
			},
			"FindByIdempotencyKey": func() error {
				_, err := txs.FindByIdempotencyKey(canceled, tx.ProviderID(), tx.IdempotencyKey())
				return err
			},
			"FindByExternalID": func() error {
				_, err := txs.FindByExternalID(canceled, tx.ProviderID(), tx.ExternalTransactionID())
				return err
			},
			"HasProcessedReversal": func() error {
				_, err := txs.HasProcessedReversal(canceled, tx.ID())
				return err
			},
			"ClaimDue": func() error {
				_, err := txs.ClaimDue(canceled, time.Minute, 10)
				return err
			},
		} {
			if err := call(); !errors.Is(err, app.ErrUnavailable) || !errors.Is(err, context.Canceled) {
				t.Errorf("%s on a canceled context: %v, want app.ErrUnavailable wrapping context.Canceled", name, err)
			}
		}
		return txs.Insert(ctx, tx)
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
}
