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

var (
	walletID = uuid.MustParse("0198a000-0000-7000-8000-000000000001")
	playerID = uuid.MustParse("0198a000-0000-7000-8000-000000000002")
	txID     = uuid.MustParse("0198a000-0000-7000-8000-000000000003")
	entryID  = uuid.MustParse("0198a000-0000-7000-8000-000000000004")
	otherTx  = uuid.MustParse("0198a000-0000-7000-8000-000000000005")
	otherEnt = uuid.MustParse("0198a000-0000-7000-8000-000000000006")

	t0 = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	t1 = t0.Add(time.Minute)
)

func minor(t *testing.T, units int64, c money.Currency) money.Money {
	t.Helper()
	m, err := money.FromMinor(units, c)
	if err != nil {
		t.Fatalf("FromMinor(%d, %q) error = %v", units, c.Code(), err)
	}
	return m
}

func currency(t *testing.T, code string) money.Currency {
	t.Helper()
	c, err := money.ParseCurrency(code)
	if err != nil {
		t.Fatalf("ParseCurrency(%q) error = %v", code, err)
	}
	return c
}

func brl(t *testing.T, units int64) money.Money { return minor(t, units, money.BRL()) }

func usd(t *testing.T, units int64) money.Money { return minor(t, units, currency(t, "USD")) }

func openWallet(t *testing.T, initial int64) *wallet.Wallet {
	t.Helper()
	w, _, err := wallet.Open(wallet.OpenParams{
		ID:             walletID,
		PlayerID:       playerID,
		Initial:        brl(t, initial),
		OpeningTxID:    txID,
		OpeningEntryID: entryID,
		Now:            t0,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return w
}

func TestOpen_PositiveCreatesOpeningEntry(t *testing.T) {
	w, entry, err := wallet.Open(wallet.OpenParams{
		ID:             walletID,
		PlayerID:       playerID,
		Initial:        brl(t, 100000),
		OpeningTxID:    txID,
		OpeningEntryID: entryID,
		Now:            t0,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if entry == nil {
		t.Fatal("Open() entry = nil, want the opening entry")
	}

	if w.ID() != walletID || w.PlayerID() != playerID {
		t.Errorf("ids = (%v, %v), want (%v, %v)", w.ID(), w.PlayerID(), walletID, playerID)
	}
	if got := w.Balance().String(); got != "1000.00" {
		t.Errorf("Balance() = %s, want 1000.00", got)
	}
	if w.Currency() != money.BRL() {
		t.Errorf("Currency() = %q, want BRL", w.Currency().Code())
	}
	if w.Version() != 1 {
		t.Errorf("Version() = %d, want 1", w.Version())
	}
	want := wallet.Snapshot{
		ID: walletID, PlayerID: playerID, Balance: brl(t, 100000),
		Version: 1, CreatedAt: t0, UpdatedAt: t0,
	}
	if got := w.Snapshot(); got != want {
		t.Errorf("Snapshot() = %+v, want %+v", got, want)
	}

	if entry.ID() != entryID || entry.WalletID() != walletID || entry.TransactionID() != txID {
		t.Errorf("entry ids = (%v, %v, %v), want (%v, %v, %v)",
			entry.ID(), entry.WalletID(), entry.TransactionID(), entryID, walletID, txID)
	}
	if entry.Direction() != wallet.DirectionCredit {
		t.Errorf("entry Direction() = %q, want %q", entry.Direction(), wallet.DirectionCredit)
	}
	if got := entry.Amount().String(); got != "1000.00" {
		t.Errorf("entry Amount() = %s, want 1000.00", got)
	}
	if got := entry.BalanceBefore().String(); got != "0.00" {
		t.Errorf("entry BalanceBefore() = %s, want 0.00", got)
	}
	if got := entry.BalanceAfter().String(); got != "1000.00" {
		t.Errorf("entry BalanceAfter() = %s, want 1000.00", got)
	}
	if !entry.CreatedAt().Equal(t0) {
		t.Errorf("entry CreatedAt() = %v, want %v", entry.CreatedAt(), t0)
	}
}

func TestOpen_ZeroHasNoEntry(t *testing.T) {
	zero, err := money.Zero(currency(t, "USD"))
	if err != nil {
		t.Fatalf("Zero() error = %v", err)
	}
	w, entry, err := wallet.Open(wallet.OpenParams{ID: walletID, PlayerID: playerID, Initial: zero, Now: t0})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if entry != nil {
		t.Errorf("Open() entry = %+v, want nil", entry)
	}
	if w.Version() != 1 {
		t.Errorf("Version() = %d, want 1", w.Version())
	}
	if !w.Balance().IsZero() || w.Currency() != currency(t, "USD") {
		t.Errorf("Balance() = %s %s, want 0.00 USD", w.Balance(), w.Currency().Code())
	}
}

func TestOpen_RejectsInvalid(t *testing.T) {
	valid := func(t *testing.T) wallet.OpenParams {
		return wallet.OpenParams{
			ID:             walletID,
			PlayerID:       playerID,
			Initial:        brl(t, 1000),
			OpeningTxID:    txID,
			OpeningEntryID: entryID,
			Now:            t0,
		}
	}
	tests := []struct {
		name   string
		mutate func(t *testing.T, p *wallet.OpenParams)
	}{
		{"nil wallet id", func(_ *testing.T, p *wallet.OpenParams) { p.ID = uuid.Nil }},
		{"nil player id", func(_ *testing.T, p *wallet.OpenParams) { p.PlayerID = uuid.Nil }},
		{"uninitialized initial", func(_ *testing.T, p *wallet.OpenParams) { p.Initial = money.Money{} }},
		{"negative initial", func(t *testing.T, p *wallet.OpenParams) { p.Initial = brl(t, -1) }},
		{"zero time", func(_ *testing.T, p *wallet.OpenParams) { p.Now = time.Time{} }},
		{"funded without opening transaction id", func(_ *testing.T, p *wallet.OpenParams) { p.OpeningTxID = uuid.Nil }},
		{"funded without opening entry id", func(_ *testing.T, p *wallet.OpenParams) { p.OpeningEntryID = uuid.Nil }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := valid(t)
			tc.mutate(t, &p)

			w, entry, err := wallet.Open(p)
			if !errors.Is(err, wallet.ErrInvalidWallet) {
				t.Fatalf("Open() error = %v, want ErrInvalidWallet", err)
			}
			if w != nil || entry != nil {
				t.Errorf("Open() = (%v, %v), want no wallet and no entry on error", w, entry)
			}
		})
	}
}

func TestDebit(t *testing.T) {
	w := openWallet(t, 10000)

	entry, err := w.Debit(brl(t, 8000), otherTx, otherEnt, t1)
	if err != nil {
		t.Fatalf("Debit() error = %v", err)
	}

	if got := w.Balance().String(); got != "20.00" {
		t.Errorf("Balance() = %s, want 20.00", got)
	}
	if w.Version() != 2 {
		t.Errorf("Version() = %d, want 2", w.Version())
	}
	if got := w.Snapshot().UpdatedAt; !got.Equal(t1) {
		t.Errorf("UpdatedAt = %v, want %v", got, t1)
	}
	if got := w.Snapshot().CreatedAt; !got.Equal(t0) {
		t.Errorf("CreatedAt = %v, want it untouched at %v", got, t0)
	}

	if entry.ID() != otherEnt || entry.WalletID() != walletID || entry.TransactionID() != otherTx {
		t.Errorf("entry ids = (%v, %v, %v), want (%v, %v, %v)",
			entry.ID(), entry.WalletID(), entry.TransactionID(), otherEnt, walletID, otherTx)
	}
	if entry.Direction() != wallet.DirectionDebit {
		t.Errorf("Direction() = %q, want %q", entry.Direction(), wallet.DirectionDebit)
	}
	if got := entry.Amount().String(); got != "80.00" {
		t.Errorf("Amount() = %s, want 80.00", got)
	}
	if before, after := entry.BalanceBefore().String(), entry.BalanceAfter().String(); before != "100.00" || after != "20.00" {
		t.Errorf("entry balance = %s -> %s, want 100.00 -> 20.00", before, after)
	}
	if !entry.CreatedAt().Equal(t1) {
		t.Errorf("CreatedAt() = %v, want %v", entry.CreatedAt(), t1)
	}
}

func TestDebit_ExactBalanceReachesZero(t *testing.T) {
	w := openWallet(t, 5000)

	entry, err := w.Debit(brl(t, 5000), otherTx, otherEnt, t1)
	if err != nil {
		t.Fatalf("Debit() error = %v", err)
	}
	if !w.Balance().IsZero() {
		t.Errorf("Balance() = %s, want 0.00", w.Balance())
	}
	if !entry.BalanceAfter().IsZero() {
		t.Errorf("entry BalanceAfter() = %s, want 0.00", entry.BalanceAfter())
	}
}

func TestDebit_InsufficientLeavesStateUnchanged(t *testing.T) {
	w := openWallet(t, 10000)
	before := w.Snapshot()

	entry, err := w.Debit(brl(t, 10001), otherTx, otherEnt, t1)
	if !errors.Is(err, wallet.ErrInsufficientFunds) {
		t.Fatalf("Debit() error = %v, want ErrInsufficientFunds", err)
	}
	if entry != (wallet.LedgerEntry{}) {
		t.Errorf("Debit() entry = %+v, want the zero entry on error", entry)
	}
	if got := w.Snapshot(); got != before {
		t.Errorf("wallet changed after a rejected debit: got %+v, want %+v", got, before)
	}
}

func TestCredit(t *testing.T) {
	w := openWallet(t, 2000)

	entry, err := w.Credit(brl(t, 505), otherTx, otherEnt, t1)
	if err != nil {
		t.Fatalf("Credit() error = %v", err)
	}

	if got := w.Balance().String(); got != "25.05" {
		t.Errorf("Balance() = %s, want 25.05", got)
	}
	if w.Version() != 2 {
		t.Errorf("Version() = %d, want 2", w.Version())
	}
	if got := w.Snapshot().UpdatedAt; !got.Equal(t1) {
		t.Errorf("UpdatedAt = %v, want %v", got, t1)
	}
	if entry.Direction() != wallet.DirectionCredit {
		t.Errorf("Direction() = %q, want %q", entry.Direction(), wallet.DirectionCredit)
	}
	if before, after := entry.BalanceBefore().String(), entry.BalanceAfter().String(); before != "20.00" || after != "25.05" {
		t.Errorf("entry balance = %s -> %s, want 20.00 -> 25.05", before, after)
	}
}

func TestCredit_Overflow(t *testing.T) {
	w := openWallet(t, math.MaxInt64)
	before := w.Snapshot()

	entry, err := w.Credit(brl(t, 1), otherTx, otherEnt, t1)
	if !errors.Is(err, wallet.ErrBalanceLimit) {
		t.Fatalf("Credit() error = %v, want ErrBalanceLimit", err)
	}
	if entry != (wallet.LedgerEntry{}) {
		t.Errorf("Credit() entry = %+v, want the zero entry on error", entry)
	}
	if got := w.Snapshot(); got != before {
		t.Errorf("wallet changed after a rejected credit: got %+v, want %+v", got, before)
	}
}

func TestCredit_ReachesTheLimitExactly(t *testing.T) {
	w := openWallet(t, math.MaxInt64-1)

	if _, err := w.Credit(brl(t, 1), otherTx, otherEnt, t1); err != nil {
		t.Fatalf("Credit() error = %v, want nil: the sum still fits in int64", err)
	}
	if got := w.Balance().Minor(); got != math.MaxInt64 {
		t.Errorf("Balance().Minor() = %d, want %d", got, int64(math.MaxInt64))
	}
}

var movements = []struct {
	name string
	run  func(w *wallet.Wallet, amount money.Money, tx, entry uuid.UUID, now time.Time) (wallet.LedgerEntry, error)
}{
	{"debit", (*wallet.Wallet).Debit},
	{"credit", (*wallet.Wallet).Credit},
}

func TestMovement_CurrencyMismatch(t *testing.T) {
	for _, m := range movements {
		t.Run(m.name, func(t *testing.T) {
			w := openWallet(t, 10000)
			before := w.Snapshot()

			_, err := m.run(w, usd(t, 100), otherTx, otherEnt, t1)
			if !errors.Is(err, wallet.ErrCurrencyMismatch) {
				t.Fatalf("error = %v, want ErrCurrencyMismatch", err)
			}
			if got := w.Snapshot(); got != before {
				t.Errorf("wallet changed after a rejected movement: got %+v, want %+v", got, before)
			}
		})
	}
}

func TestMovement_ZeroAmountRejected(t *testing.T) {
	tests := []struct {
		name   string
		amount func(t *testing.T) money.Money
	}{
		{"zero", func(t *testing.T) money.Money { return brl(t, 0) }},
		{"negative", func(t *testing.T) money.Money { return brl(t, -100) }},
		{"uninitialized", func(*testing.T) money.Money { return money.Money{} }},
		{"zero in another currency", func(t *testing.T) money.Money { return usd(t, 0) }},
	}
	for _, m := range movements {
		for _, tc := range tests {
			t.Run(m.name+"/"+tc.name, func(t *testing.T) {
				w := openWallet(t, 10000)
				before := w.Snapshot()

				_, err := m.run(w, tc.amount(t), otherTx, otherEnt, t1)
				if !errors.Is(err, wallet.ErrNonPositiveAmount) {
					t.Fatalf("error = %v, want ErrNonPositiveAmount", err)
				}
				if got := w.Snapshot(); got != before {
					t.Errorf("wallet changed after a rejected movement: got %+v, want %+v", got, before)
				}
			})
		}
	}
}

func TestMovement_InvalidEntryIsAtomic(t *testing.T) {
	tests := []struct {
		name  string
		tx    uuid.UUID
		entry uuid.UUID
		now   time.Time
	}{
		{"nil transaction id", uuid.Nil, otherEnt, t1},
		{"nil entry id", otherTx, uuid.Nil, t1},
		{"zero time", otherTx, otherEnt, time.Time{}},
	}
	for _, m := range movements {
		for _, tc := range tests {
			t.Run(m.name+"/"+tc.name, func(t *testing.T) {
				w := openWallet(t, 10000)
				before := w.Snapshot()

				_, err := m.run(w, brl(t, 100), tc.tx, tc.entry, tc.now)
				if !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
					t.Fatalf("error = %v, want ErrInvalidLedgerEntry", err)
				}
				if got := w.Snapshot(); got != before {
					t.Errorf("wallet changed after a rejected movement: got %+v, want %+v", got, before)
				}
			})
		}
	}
}

func TestMovement_ZeroWalletFailsLoudly(t *testing.T) {
	for _, m := range movements {
		t.Run(m.name, func(t *testing.T) {
			var w wallet.Wallet

			_, err := m.run(&w, brl(t, 100), otherTx, otherEnt, t1)
			if !errors.Is(err, money.ErrUninitialized) {
				t.Fatalf("error = %v, want money.ErrUninitialized", err)
			}
			if w.Version() != 0 || w.Balance().IsValid() {
				t.Errorf("zero wallet changed: version %d, balance valid %t", w.Version(), w.Balance().IsValid())
			}
		})
	}
}

func TestMovement_UpdatedAtNeverMovesBack(t *testing.T) {
	for _, m := range movements {
		t.Run(m.name, func(t *testing.T) {
			w := openWallet(t, 10000)
			if _, err := w.Credit(brl(t, 100), otherTx, otherEnt, t1); err != nil {
				t.Fatalf("Credit() error = %v", err)
			}

			skewed := t0.Add(-time.Second)
			entry, err := m.run(w, brl(t, 100), txID, entryID, skewed)
			if err != nil {
				t.Fatalf("error = %v", err)
			}

			if got := w.Snapshot().UpdatedAt; !got.Equal(t1) {
				t.Errorf("UpdatedAt = %v, want it kept at %v", got, t1)
			}
			if !entry.CreatedAt().Equal(skewed) {
				t.Errorf("entry CreatedAt() = %v, want the movement time %v", entry.CreatedAt(), skewed)
			}
			if _, err := wallet.Rehydrate(w.Snapshot()); err != nil {
				t.Errorf("Rehydrate(Snapshot()) error = %v, want a loadable wallet", err)
			}
		})
	}
}

func TestWallet_LedgerChainsAcrossMovements(t *testing.T) {
	w, opening, err := wallet.Open(wallet.OpenParams{
		ID: walletID, PlayerID: playerID, Initial: brl(t, 1000),
		OpeningTxID: txID, OpeningEntryID: entryID, Now: t0,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	entries := []wallet.LedgerEntry{*opening}

	steps := []struct {
		debit  bool
		amount int64
	}{
		{false, 250}, {true, 1100}, {true, 150}, {false, 1}, {true, 1}, {false, 99999},
	}
	var sum int64 = 1000
	for i, s := range steps {
		var e wallet.LedgerEntry
		now := t0.Add(time.Duration(i+1) * time.Second)
		if s.debit {
			e, err = w.Debit(brl(t, s.amount), txID, entryID, now)
			sum -= s.amount
		} else {
			e, err = w.Credit(brl(t, s.amount), txID, entryID, now)
			sum += s.amount
		}
		if err != nil {
			t.Fatalf("step %d error = %v", i, err)
		}
		entries = append(entries, e)
	}

	for i := 1; i < len(entries); i++ {
		if !entries[i].BalanceBefore().Equal(entries[i-1].BalanceAfter()) {
			t.Errorf("entry %d starts at %s, previous ended at %s",
				i, entries[i].BalanceBefore(), entries[i-1].BalanceAfter())
		}
	}
	if last := entries[len(entries)-1].BalanceAfter(); !last.Equal(w.Balance()) {
		t.Errorf("last entry ends at %s, wallet holds %s", last, w.Balance())
	}
	if got := w.Balance().Minor(); got != sum {
		t.Errorf("Balance().Minor() = %d, want %d", got, sum)
	}
	if got, want := w.Version(), int64(1+len(steps)); got != want {
		t.Errorf("Version() = %d, want %d", got, want)
	}
}

func TestRehydrate_NoSideEffects(t *testing.T) {
	snap := wallet.Snapshot{
		ID: walletID, PlayerID: playerID, Balance: usd(t, 4250),
		Version: 7, CreatedAt: t0, UpdatedAt: t1,
	}

	w, err := wallet.Rehydrate(snap)
	if err != nil {
		t.Fatalf("Rehydrate() error = %v", err)
	}
	if got := w.Snapshot(); got != snap {
		t.Errorf("Snapshot() = %+v, want the original %+v", got, snap)
	}
	if w.Version() != 7 {
		t.Errorf("Version() = %d, want 7: loading must not bump it", w.Version())
	}
	if w.Currency() != currency(t, "USD") {
		t.Errorf("Currency() = %q, want USD", w.Currency().Code())
	}
}

func TestRehydrate_AcceptsZeroBalanceAndEqualTimestamps(t *testing.T) {
	snap := wallet.Snapshot{
		ID: walletID, PlayerID: playerID, Balance: brl(t, 0),
		Version: 1, CreatedAt: t0, UpdatedAt: t0,
	}
	if _, err := wallet.Rehydrate(snap); err != nil {
		t.Fatalf("Rehydrate() error = %v", err)
	}
}

func TestRehydrate_RejectsNegativeBalance(t *testing.T) {
	_, err := wallet.Rehydrate(wallet.Snapshot{
		ID: walletID, PlayerID: playerID, Balance: brl(t, -1),
		Version: 1, CreatedAt: t0, UpdatedAt: t0,
	})
	if !errors.Is(err, wallet.ErrInvalidWallet) {
		t.Fatalf("Rehydrate() error = %v, want ErrInvalidWallet", err)
	}
}

func TestRehydrate_RejectsInvalidSnapshot(t *testing.T) {
	valid := func(t *testing.T) wallet.Snapshot {
		return wallet.Snapshot{
			ID: walletID, PlayerID: playerID, Balance: brl(t, 1000),
			Version: 3, CreatedAt: t0, UpdatedAt: t1,
		}
	}
	tests := []struct {
		name   string
		mutate func(t *testing.T, s *wallet.Snapshot)
	}{
		{"nil wallet id", func(_ *testing.T, s *wallet.Snapshot) { s.ID = uuid.Nil }},
		{"nil player id", func(_ *testing.T, s *wallet.Snapshot) { s.PlayerID = uuid.Nil }},
		{"uninitialized balance", func(_ *testing.T, s *wallet.Snapshot) { s.Balance = money.Money{} }},
		{"zero version", func(_ *testing.T, s *wallet.Snapshot) { s.Version = 0 }},
		{"negative version", func(_ *testing.T, s *wallet.Snapshot) { s.Version = -1 }},
		{"zero created at", func(_ *testing.T, s *wallet.Snapshot) { s.CreatedAt = time.Time{} }},
		{"zero updated at", func(_ *testing.T, s *wallet.Snapshot) { s.UpdatedAt = time.Time{} }},
		{"updated before created", func(_ *testing.T, s *wallet.Snapshot) { s.UpdatedAt = s.CreatedAt.Add(-time.Nanosecond) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := valid(t)
			tc.mutate(t, &s)

			w, err := wallet.Rehydrate(s)
			if !errors.Is(err, wallet.ErrInvalidWallet) {
				t.Fatalf("Rehydrate() error = %v, want ErrInvalidWallet", err)
			}
			if w != nil {
				t.Errorf("Rehydrate() = %+v, want nil on error", w)
			}
		})
	}
}

func TestSentinelErrorsAreDistinct(t *testing.T) {
	sentinels := []error{
		wallet.ErrInvalidWallet,
		wallet.ErrInsufficientFunds,
		wallet.ErrCurrencyMismatch,
		wallet.ErrNonPositiveAmount,
		wallet.ErrBalanceLimit,
		wallet.ErrInvalidLedgerEntry,
	}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if (i == j) != errors.Is(a, b) {
				t.Errorf("errors.Is(%v, %v) = %t, want %t", a, b, errors.Is(a, b), i == j)
			}
		}
	}
}
