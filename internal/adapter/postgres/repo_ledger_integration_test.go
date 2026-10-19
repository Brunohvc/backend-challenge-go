//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/postgres"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

func ledgerPage(t *testing.T, uow app.UnitOfWork, id uuid.UUID, afterSeq int64, limit int) []app.LedgerRecord {
	t.Helper()
	var records []app.LedgerRecord
	err := uow.Snapshot(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		records, err = r.Ledger().Page(ctx, id, afterSeq, limit)
		return err
	})
	if err != nil {
		t.Fatalf("Page(%s, %d, %d) error = %v", id, afterSeq, limit, err)
	}
	return records
}

func ledgerTotals(t *testing.T, uow app.UnitOfWork, id uuid.UUID) (app.LedgerTotals, error) {
	t.Helper()
	var totals app.LedgerTotals
	err := uow.Snapshot(t.Context(), func(ctx context.Context, r app.Repos) error {
		var err error
		totals, err = r.Ledger().Totals(ctx, id)
		return err
	})
	return totals, err
}

func requireEntry(t *testing.T, got, want wallet.LedgerEntry) {
	t.Helper()
	if got.ID() != want.ID() || got.WalletID() != want.WalletID() || got.TransactionID() != want.TransactionID() ||
		got.Direction() != want.Direction() || !got.Amount().Equal(want.Amount()) ||
		!got.BalanceBefore().Equal(want.BalanceBefore()) || !got.BalanceAfter().Equal(want.BalanceAfter()) ||
		!got.CreatedAt().Equal(want.CreatedAt()) || got.CreatedAt().Location() != time.UTC {
		t.Fatalf("entry = %+v, want %+v", got, want)
	}
}

func requireRecords(t *testing.T, got []app.LedgerRecord, want []wallet.LedgerEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range got {
		requireEntry(t, got[i].Entry, want[i])
		if i > 0 && got[i].Seq <= got[i-1].Seq {
			t.Fatalf("seq %d after seq %d", got[i].Seq, got[i-1].Seq)
		}
	}
}

func TestLedgerRepo_PageStableOrder(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, opening := openWallet(t, uow, 1000)
	other, _ := openWallet(t, uow, 500)
	want := []wallet.LedgerEntry{*opening}
	for _, m := range []struct {
		kind   string
		amount int64
	}{{"BET", 100}, {"WIN", 40}, {"BET", 200}, {"WIN", 15}} {
		want = append(want, moveWallet(t, uow, w.ID(), m.kind, m.amount))
		moveWallet(t, uow, other.ID(), "BET", 10)
	}

	var got []app.LedgerRecord
	cursor := int64(0)
	for i, size := range []int{2, 2, 1} {
		page := ledgerPage(t, uow, w.ID(), cursor, 2)
		if len(page) != size {
			t.Fatalf("page %d has %d entries, want %d", i+1, len(page), size)
		}
		got = append(got, page...)
		cursor = page[len(page)-1].Seq
	}
	requireRecords(t, got, want)
	if page := ledgerPage(t, uow, w.ID(), cursor, 2); len(page) != 0 {
		t.Fatalf("a page past the last entry has %d entries", len(page))
	}

	appended := moveWallet(t, uow, w.ID(), "BET", 5)
	requireRecords(t, ledgerPage(t, uow, w.ID(), 0, 2), want[:2])
	requireRecords(t, ledgerPage(t, uow, w.ID(), got[1].Seq, 2), want[2:4])
	requireRecords(t, ledgerPage(t, uow, w.ID(), cursor, 2), []wallet.LedgerEntry{appended})
	requireRecords(t, ledgerPage(t, uow, w.ID(), 0, 100), append(want, appended))
}

func TestLedgerRepo_PageRefusesAnEmptyLimit(t *testing.T) {
	uow, _ := newUnitOfWork(t)
	for _, limit := range []int{0, -1} {
		err := uow.Snapshot(t.Context(), func(ctx context.Context, r app.Repos) error {
			_, err := r.Ledger().Page(ctx, newID(), 0, limit)
			return err
		})
		requireClass(t, err, app.ErrPermanent)
	}
}

func TestLedgerRepo_Totals(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 1000)
	moveWallet(t, uow, w.ID(), "BET", 300)
	moveWallet(t, uow, w.ID(), "WIN", 50)
	moveWallet(t, uow, w.ID(), "BET", 25)

	totals, err := ledgerTotals(t, uow, w.ID())
	if err != nil {
		t.Fatalf("Totals() error = %v", err)
	}
	if want := (app.LedgerTotals{NetMinor: 1050 - 325, Count: 4}); totals != want {
		t.Fatalf("Totals() = %+v, want %+v", totals, want)
	}
	if balance := mustLoadWallet(t, uow, w.ID()).Balance().Minor(); balance != totals.NetMinor {
		t.Fatalf("balance %d, credits - debits %d", balance, totals.NetMinor)
	}
}

func TestLedgerRepo_TotalsZeroEntries(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	busy, _ := openWallet(t, uow, 1000)
	moveWallet(t, uow, busy.ID(), "BET", 100)
	empty, opening := openWallet(t, uow, 0)
	if opening != nil {
		t.Fatalf("a wallet opened with 0.00 got an opening entry: %+v", opening)
	}

	for name, id := range map[string]uuid.UUID{"zero balance": empty.ID(), "unknown wallet": newID()} {
		totals, err := ledgerTotals(t, uow, id)
		if err != nil {
			t.Fatalf("%s: Totals() error = %v", name, err)
		}
		if totals != (app.LedgerTotals{}) {
			t.Fatalf("%s: Totals() = %+v, want 0/0/0", name, totals)
		}
		if page := ledgerPage(t, uow, id, 0, 10); len(page) != 0 {
			t.Fatalf("%s: ledger has %d entries", name, len(page))
		}
	}
	if balance := mustLoadWallet(t, uow, empty.ID()).Balance(); !balance.IsZero() {
		t.Fatalf("balance = %s, want 0.00", balance)
	}
}

func TestLedgerRepo_TotalsBeyondInt64Turnover(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, math.MaxInt64)
	moveWallet(t, uow, w.ID(), "BET", math.MaxInt64)
	moveWallet(t, uow, w.ID(), "WIN", math.MaxInt64)

	totals, err := ledgerTotals(t, uow, w.ID())
	if err != nil {
		t.Fatalf("Totals() error = %v, want the net of credits that add up past int64", err)
	}
	if want := (app.LedgerTotals{NetMinor: math.MaxInt64, Count: 3}); totals != want {
		t.Fatalf("Totals() = %+v, want %+v", totals, want)
	}
}

func TestLedgerRepo_TotalsNetOverflowIsAnError(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	super := connect(t, pg.SuperDSN)
	mustExec(t, super, "SET session_replication_role = replica")
	w := walletRow(math.MaxInt64)
	mustInsert(t, super, "wallets", w)
	for _, credit := range []row{openingRow(w), externalRow(w, "WIN", math.MaxInt64)} {
		mustInsert(t, super, "wager_transactions", credit)
		mustInsert(t, super, "wallet_ledger_entries", ledgerRow(credit, "CREDIT", 0))
	}

	_, err := ledgerTotals(t, uow, get[uuid.UUID](w, "id"))
	requireClass(t, err, app.ErrPermanent)
	requireSQLState(t, err, "22003", "")
}

func TestLedgerRepo_AppendRefusesABrokenChain(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	w, _ := openWallet(t, uow, 1000)

	err := uow.Do(t.Context(), func(ctx context.Context, r app.Repos) error {
		locked, err := r.Wallets().GetForUpdate(ctx, w.ID())
		if err != nil {
			return err
		}
		bet := externalRow(walletFields(locked), "BET", 100)
		if err := insert(ctx, postgres.TxOf(r), "wager_transactions", bet); err != nil {
			return err
		}
		entry, err := wallet.NewLedgerEntry(newID(), w.ID(), get[uuid.UUID](bet, "id"),
			wallet.DirectionDebit, brl(t, 100), brl(t, 500), brl(t, 400), t1)
		if err != nil {
			return err
		}
		return r.Ledger().Append(ctx, entry)
	})
	requireClass(t, err, app.ErrPermanent)
	requireSQLState(t, err, ledgerMismatch, "ledger_entry_check")
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && strings.Contains(err.Error(), pgErr.Message) {
		t.Fatalf("error = %q, repeats the server's message %q", err, pgErr.Message)
	}

	if totals, err := ledgerTotals(t, uow, w.ID()); err != nil || totals.Count != 1 {
		t.Fatalf("Totals() = %+v, %v; want the opening entry alone", totals, err)
	}
}

func TestLedgerRepo_UnloadableRowIsPermanent(t *testing.T) {
	resetDB(t)
	uow, _ := newUnitOfWork(t)
	gbp := walletRow(1000).set("currency", "GBP")
	opening := openingRow(gbp)
	conn := connect(t, pg.AppDSN)
	tx := begin(t, conn)
	mustInsert(t, tx, "wallets", gbp)
	mustInsert(t, tx, "wager_transactions", opening)
	mustInsert(t, tx, "wallet_ledger_entries", ledgerRow(opening, "CREDIT", 0))
	mustCommit(t, tx)

	err := uow.Snapshot(t.Context(), func(ctx context.Context, r app.Repos) error {
		_, err := r.Ledger().Page(ctx, get[uuid.UUID](gbp, "id"), 0, 10)
		return err
	})
	requireClass(t, err, app.ErrPermanent)
	if !errors.Is(err, money.ErrUnsupportedCurrency) {
		t.Fatalf("Page() error = %v, want it to say why the row does not load", err)
	}
}
