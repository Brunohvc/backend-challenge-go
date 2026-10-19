// Package app holds the ports the use cases need from the outside world and the adapter errors.
package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

// Clock tells the time that is persisted, published or used in a business decision.
type Clock interface {
	Now() time.Time
}

// IDGenerator hands out the identifiers of new wallets, transactions, entries and events.
type IDGenerator interface {
	New() uuid.UUID
}

// UnitOfWork runs the reads and writes of a use case in one database transaction.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context, r Repos) error) error
	Snapshot(ctx context.Context, fn func(ctx context.Context, r Repos) error) error
}

// Repos gives access to the repositories bound to one unit of work.
type Repos interface {
	Wallets() WalletRepository
	Ledger() LedgerRepository
	Transactions() TransactionRepository
	Outbox() OutboxRepository
	Inbox() InboxRepository
}

// WalletRepository stores wallets.
type WalletRepository interface {
	Insert(ctx context.Context, w *wallet.Wallet) error
	Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	IDByPlayerAndCurrency(ctx context.Context, player uuid.UUID, currency money.Currency) (uuid.UUID, error)
	UpdateBalance(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error
}

// LedgerRecord is a ledger entry with its position in the ledger.
type LedgerRecord struct {
	Seq   int64
	Entry wallet.LedgerEntry
}

// LedgerTotals adds up the ledger of a wallet, in minor units of its currency.
type LedgerTotals struct {
	NetMinor int64
	Count    int64
}

// LedgerRepository stores the append-only ledger of the wallets.
type LedgerRepository interface {
	Append(ctx context.Context, e wallet.LedgerEntry) error
	Page(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]LedgerRecord, error)
	Totals(ctx context.Context, walletID uuid.UUID) (LedgerTotals, error)
}
