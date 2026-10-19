package wallet

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
)

// Direction says whether a ledger entry took money out of the wallet or put it in.
type Direction string

// The directions of a ledger entry.
const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

func (d Direction) valid() bool {
	return d == DirectionDebit || d == DirectionCredit
}

func (d Direction) apply(balance, amount money.Money) (money.Money, error) {
	if d == DirectionDebit {
		return balance.Sub(amount)
	}
	return balance.Add(amount)
}

// LedgerEntry is one immutable line of a wallet's ledger.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// NewLedgerEntry builds a ledger entry, enforcing every invariant of the ledger.
func NewLedgerEntry(id, walletID, txID uuid.UUID, dir Direction, amount, before, after money.Money, at time.Time) (LedgerEntry, error) {
	switch {
	case id == uuid.Nil:
		return LedgerEntry{}, invalidEntry("id is nil")
	case walletID == uuid.Nil:
		return LedgerEntry{}, invalidEntry("wallet id is nil")
	case txID == uuid.Nil:
		return LedgerEntry{}, invalidEntry("transaction id is nil")
	case !dir.valid():
		return LedgerEntry{}, invalidEntry("unknown direction")
	case !amount.IsPositive():
		return LedgerEntry{}, invalidEntry("amount must be positive")
	case !before.IsValid() || before.IsNegative():
		return LedgerEntry{}, invalidEntry("balance before must be a non-negative amount")
	case !after.IsValid() || after.IsNegative():
		return LedgerEntry{}, invalidEntry("balance after must be a non-negative amount")
	case at.IsZero():
		return LedgerEntry{}, invalidEntry("created at is zero")
	}

	want, err := dir.apply(before, amount)
	if err != nil {
		return LedgerEntry{}, fmt.Errorf("%w: balance after cannot be computed: %w", ErrInvalidLedgerEntry, err)
	}
	if !after.Equal(want) {
		return LedgerEntry{}, invalidEntry("balance after does not follow from the amount and the currency")
	}

	return LedgerEntry{
		id:            id,
		walletID:      walletID,
		transactionID: txID,
		direction:     dir,
		amount:        amount,
		balanceBefore: before,
		balanceAfter:  after,
		createdAt:     at,
	}, nil
}

// ID returns the identifier of the entry.
func (e LedgerEntry) ID() uuid.UUID { return e.id }

// WalletID returns the identifier of the wallet the entry belongs to.
func (e LedgerEntry) WalletID() uuid.UUID { return e.walletID }

// TransactionID returns the identifier of the transaction that caused the entry.
func (e LedgerEntry) TransactionID() uuid.UUID { return e.transactionID }

// Direction returns whether the entry debited or credited the wallet.
func (e LedgerEntry) Direction() Direction { return e.direction }

// Amount returns the amount that moved, always positive.
func (e LedgerEntry) Amount() money.Money { return e.amount }

// BalanceBefore returns the wallet balance just before the movement.
func (e LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }

// BalanceAfter returns the wallet balance just after the movement.
func (e LedgerEntry) BalanceAfter() money.Money { return e.balanceAfter }

// CreatedAt returns when the movement happened.
func (e LedgerEntry) CreatedAt() time.Time { return e.createdAt }
