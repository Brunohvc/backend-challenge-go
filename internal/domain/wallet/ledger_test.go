package wallet_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

func TestNewLedgerEntry_Valid(t *testing.T) {
	tests := []struct {
		name                  string
		dir                   wallet.Direction
		amount, before, after int64
	}{
		{"credit", wallet.DirectionCredit, 500, 1000, 1500},
		{"credit from zero", wallet.DirectionCredit, 1000, 0, 1000},
		{"debit", wallet.DirectionDebit, 500, 1500, 1000},
		{"debit to zero", wallet.DirectionDebit, 1000, 1000, 0},
		{"credit to the limit", wallet.DirectionCredit, 1, math.MaxInt64 - 1, math.MaxInt64},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, err := wallet.NewLedgerEntry(entryID, walletID, txID, tc.dir,
				brl(t, tc.amount), brl(t, tc.before), brl(t, tc.after), t0)
			if err != nil {
				t.Fatalf("NewLedgerEntry() error = %v", err)
			}

			if e.ID() != entryID || e.WalletID() != walletID || e.TransactionID() != txID {
				t.Errorf("ids = (%v, %v, %v), want (%v, %v, %v)",
					e.ID(), e.WalletID(), e.TransactionID(), entryID, walletID, txID)
			}
			if e.Direction() != tc.dir {
				t.Errorf("Direction() = %q, want %q", e.Direction(), tc.dir)
			}
			if got := e.Amount().Minor(); got != tc.amount {
				t.Errorf("Amount() = %d, want %d", got, tc.amount)
			}
			if got := e.BalanceBefore().Minor(); got != tc.before {
				t.Errorf("BalanceBefore() = %d, want %d", got, tc.before)
			}
			if got := e.BalanceAfter().Minor(); got != tc.after {
				t.Errorf("BalanceAfter() = %d, want %d", got, tc.after)
			}
			if !e.CreatedAt().Equal(t0) {
				t.Errorf("CreatedAt() = %v, want %v", e.CreatedAt(), t0)
			}
		})
	}
}

func TestNewLedgerEntry_ValidatesArithmetic(t *testing.T) {
	type args struct {
		id, walletID, txID    uuid.UUID
		dir                   wallet.Direction
		amount, before, after money.Money
		at                    time.Time
	}
	base := func(t *testing.T) args {
		return args{
			id: entryID, walletID: walletID, txID: txID,
			dir:    wallet.DirectionCredit,
			amount: brl(t, 500), before: brl(t, 1000), after: brl(t, 1500),
			at: t0,
		}
	}
	tests := []struct {
		name   string
		mutate func(t *testing.T, a *args)
	}{
		{"credit that does not add up", func(t *testing.T, a *args) { a.after = brl(t, 1400) }},
		{"credit that adds too much", func(t *testing.T, a *args) { a.after = brl(t, 1600) }},
		{"credit that lowers the balance", func(t *testing.T, a *args) { a.after = brl(t, 500) }},
		{"debit that does not add up", func(t *testing.T, a *args) {
			a.dir, a.before, a.after = wallet.DirectionDebit, brl(t, 1500), brl(t, 1100)
		}},
		{"debit that raises the balance", func(t *testing.T, a *args) {
			a.dir, a.before, a.after = wallet.DirectionDebit, brl(t, 1000), brl(t, 1500)
		}},
		{"debit past zero", func(t *testing.T, a *args) {
			a.dir, a.amount, a.before, a.after = wallet.DirectionDebit, brl(t, 1000), brl(t, 500), brl(t, -500)
		}},
		{"credit overflow", func(t *testing.T, a *args) {
			a.amount, a.before, a.after = brl(t, 1), brl(t, math.MaxInt64), brl(t, math.MaxInt64)
		}},
		{"negative before", func(t *testing.T, a *args) {
			a.before, a.after = brl(t, -500), brl(t, 0)
		}},
		{"negative after", func(t *testing.T, a *args) {
			a.dir, a.before, a.after = wallet.DirectionDebit, brl(t, 100), brl(t, -400)
		}},
		{"uninitialized before", func(_ *testing.T, a *args) { a.before = money.Money{} }},
		{"uninitialized after", func(_ *testing.T, a *args) { a.after = money.Money{} }},
		{"amount in another currency", func(t *testing.T, a *args) { a.amount = usd(t, 500) }},
		{"before in another currency", func(t *testing.T, a *args) { a.before = usd(t, 1000) }},
		{"after in another currency", func(t *testing.T, a *args) { a.after = usd(t, 1500) }},
		{"three different currencies", func(t *testing.T, a *args) {
			a.amount, a.before, a.after = usd(t, 500), minor(t, 1000, currency(t, "EUR")), brl(t, 1500)
		}},
		{"zero amount", func(t *testing.T, a *args) { a.amount, a.after = brl(t, 0), brl(t, 1000) }},
		{"negative amount", func(t *testing.T, a *args) { a.amount, a.after = brl(t, -500), brl(t, 500) }},
		{"uninitialized amount", func(_ *testing.T, a *args) { a.amount = money.Money{} }},
		{"unknown direction", func(_ *testing.T, a *args) { a.dir = "TRANSFER" }},
		{"empty direction", func(_ *testing.T, a *args) { a.dir = "" }},
		{"lower-case direction", func(_ *testing.T, a *args) { a.dir = "credit" }},
		{"nil entry id", func(_ *testing.T, a *args) { a.id = uuid.Nil }},
		{"nil wallet id", func(_ *testing.T, a *args) { a.walletID = uuid.Nil }},
		{"nil transaction id", func(_ *testing.T, a *args) { a.txID = uuid.Nil }},
		{"zero time", func(_ *testing.T, a *args) { a.at = time.Time{} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := base(t)
			tc.mutate(t, &a)

			e, err := wallet.NewLedgerEntry(a.id, a.walletID, a.txID, a.dir, a.amount, a.before, a.after, a.at)
			if !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
				t.Fatalf("NewLedgerEntry() error = %v, want ErrInvalidLedgerEntry", err)
			}
			if e != (wallet.LedgerEntry{}) {
				t.Errorf("NewLedgerEntry() = %+v, want the zero entry on error", e)
			}
		})
	}
}

func TestNewLedgerEntry_BaseCaseIsValid(t *testing.T) {
	_, err := wallet.NewLedgerEntry(entryID, walletID, txID, wallet.DirectionCredit,
		brl(t, 500), brl(t, 1000), brl(t, 1500), t0)
	if err != nil {
		t.Fatalf("NewLedgerEntry() error = %v, want nil", err)
	}
}

func TestNewLedgerEntry_OverflowKeepsItsCause(t *testing.T) {
	_, err := wallet.NewLedgerEntry(entryID, walletID, txID, wallet.DirectionCredit,
		brl(t, 1), brl(t, math.MaxInt64), brl(t, math.MaxInt64), t0)
	if !errors.Is(err, wallet.ErrInvalidLedgerEntry) || !errors.Is(err, money.ErrOverflow) {
		t.Fatalf("NewLedgerEntry() error = %v, want ErrInvalidLedgerEntry wrapping money.ErrOverflow", err)
	}
}

func TestDirection_Values(t *testing.T) {
	if wallet.DirectionDebit != "DEBIT" || wallet.DirectionCredit != "CREDIT" {
		t.Errorf("directions = (%q, %q), want (DEBIT, CREDIT)", wallet.DirectionDebit, wallet.DirectionCredit)
	}
}
