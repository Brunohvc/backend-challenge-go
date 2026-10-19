package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

// DueTransaction is a transaction ClaimDue handed to the resolver.
type DueTransaction struct {
	ID         uuid.UUID
	WalletID   uuid.UUID
	LeaseUntil time.Time
}

// TransactionRepository stores wagering transactions.
type TransactionRepository interface {
	Insert(ctx context.Context, t *wager.Transaction) error
	Update(ctx context.Context, t *wager.Transaction) error
	Get(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	FindByIdempotencyKey(ctx context.Context, providerID, key string) (*wager.Transaction, error)
	FindByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	HasProcessedReversal(ctx context.Context, referenceTxID uuid.UUID) (bool, error)
	ClaimDue(ctx context.Context, lease time.Duration, limit int) ([]DueTransaction, error)
}
