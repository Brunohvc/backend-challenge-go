// Package wallet is the aggregate that owns a player's balance and its ledger entries.
package wallet

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
)

// Wallet is a player's balance in a single currency.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// OpenParams are the inputs of Open.
type OpenParams struct {
	ID             uuid.UUID
	PlayerID       uuid.UUID
	Initial        money.Money
	OpeningTxID    uuid.UUID
	OpeningEntryID uuid.UUID
	Now            time.Time
}

// Open creates a wallet at version 1, with a CREDIT entry when Initial is positive.
func Open(p OpenParams) (*Wallet, *LedgerEntry, error) {
	w := &Wallet{
		id:        p.ID,
		playerID:  p.PlayerID,
		balance:   p.Initial,
		version:   1,
		createdAt: p.Now,
		updatedAt: p.Now,
	}
	if err := w.validate(); err != nil {
		return nil, nil, err
	}
	if p.Initial.IsZero() {
		return w, nil, nil
	}
	if p.OpeningTxID == uuid.Nil || p.OpeningEntryID == uuid.Nil {
		return nil, nil, invalidWallet("opening transaction and entry ids are required for a funded wallet")
	}

	zero, err := money.Zero(p.Initial.Currency())
	if err != nil {
		return nil, nil, fmt.Errorf("%w: opening balance: %w", ErrInvalidWallet, err)
	}
	entry, err := NewLedgerEntry(p.OpeningEntryID, p.ID, p.OpeningTxID, DirectionCredit, p.Initial, zero, p.Initial, p.Now)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: opening entry: %w", ErrInvalidWallet, err)
	}
	return w, &entry, nil
}

// Snapshot is the persistent state of a Wallet.
type Snapshot struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Rehydrate rebuilds a Wallet from stored state, validating the snapshot.
func Rehydrate(s Snapshot) (*Wallet, error) {
	w := &Wallet{
		id:        s.ID,
		playerID:  s.PlayerID,
		balance:   s.Balance,
		version:   s.Version,
		createdAt: s.CreatedAt,
		updatedAt: s.UpdatedAt,
	}
	if err := w.validate(); err != nil {
		return nil, err
	}
	return w, nil
}

// Snapshot returns the persistent state of the wallet.
func (w *Wallet) Snapshot() Snapshot {
	return Snapshot{
		ID:        w.id,
		PlayerID:  w.playerID,
		Balance:   w.balance,
		Version:   w.version,
		CreatedAt: w.createdAt,
		UpdatedAt: w.updatedAt,
	}
}

// ID returns the identifier of the wallet.
func (w *Wallet) ID() uuid.UUID { return w.id }

// PlayerID returns the identifier of the player who owns the wallet.
func (w *Wallet) PlayerID() uuid.UUID { return w.playerID }

// Currency returns the currency of the wallet, which never changes.
func (w *Wallet) Currency() money.Currency { return w.balance.Currency() }

// Balance returns the current balance. It is never negative.
func (w *Wallet) Balance() money.Money { return w.balance }

// Version returns the optimistic-locking token, starting at 1 when the wallet is opened.
func (w *Wallet) Version() int64 { return w.version }

// Debit takes amount out of the wallet and returns the ledger entry that records it.
func (w *Wallet) Debit(amount money.Money, txID, entryID uuid.UUID, now time.Time) (LedgerEntry, error) {
	return w.move(DirectionDebit, amount, txID, entryID, now)
}

// Credit puts amount into the wallet and returns the ledger entry that records it.
func (w *Wallet) Credit(amount money.Money, txID, entryID uuid.UUID, now time.Time) (LedgerEntry, error) {
	return w.move(DirectionCredit, amount, txID, entryID, now)
}

func (w *Wallet) move(dir Direction, amount money.Money, txID, entryID uuid.UUID, now time.Time) (LedgerEntry, error) {
	if !amount.IsPositive() {
		return LedgerEntry{}, ErrNonPositiveAmount
	}

	after, err := dir.apply(w.balance, amount)
	switch {
	case errors.Is(err, money.ErrCurrencyMismatch):
		return LedgerEntry{}, ErrCurrencyMismatch
	case errors.Is(err, money.ErrOverflow):
		return LedgerEntry{}, ErrBalanceLimit
	case err != nil:
		return LedgerEntry{}, fmt.Errorf("wallet: %s: %w", dir, err)
	case after.IsNegative():
		return LedgerEntry{}, ErrInsufficientFunds
	}

	entry, err := NewLedgerEntry(entryID, w.id, txID, dir, amount, w.balance, after, now)
	if err != nil {
		return LedgerEntry{}, err
	}

	w.balance = after
	w.version++
	w.advanceUpdatedAt(now)
	return entry, nil
}

func (w *Wallet) advanceUpdatedAt(now time.Time) {
	if now.After(w.updatedAt) {
		w.updatedAt = now
	}
}

func (w *Wallet) validate() error {
	switch {
	case w.id == uuid.Nil:
		return invalidWallet("id is nil")
	case w.playerID == uuid.Nil:
		return invalidWallet("player id is nil")
	case !w.balance.IsValid():
		return invalidWallet("balance has no currency")
	case w.balance.IsNegative():
		return invalidWallet("balance is negative")
	case w.version < 1:
		return invalidWallet("version must be at least 1")
	case w.createdAt.IsZero():
		return invalidWallet("created at is zero")
	case w.updatedAt.IsZero():
		return invalidWallet("updated at is zero")
	case w.updatedAt.Before(w.createdAt):
		return invalidWallet("updated at is before created at")
	}
	return nil
}
