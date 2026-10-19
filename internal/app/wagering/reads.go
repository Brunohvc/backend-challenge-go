package wagering

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

// Get returns the transaction with id, for the internal back office.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (wager.Snapshot, error) {
	return s.read(ctx, func(ctx context.Context, txs app.TransactionRepository) (*wager.Transaction, error) {
		return txs.Get(ctx, id)
	})
}

// GetForProvider returns the transaction with id only if providerID sent it.
func (s *Service) GetForProvider(ctx context.Context, id uuid.UUID, providerID string) (wager.Snapshot, error) {
	snap, err := s.Get(ctx, id)
	if err != nil {
		return wager.Snapshot{}, err
	}
	if snap.Origin != wager.OriginExternal || snap.ProviderID != providerID {
		return wager.Snapshot{}, fmt.Errorf("%w: transaction %s is not one provider %q sent", ErrTransactionNotFound, id, providerID)
	}
	return snap, nil
}

// GetByExternal returns the transaction providerID sent as externalID.
func (s *Service) GetByExternal(ctx context.Context, providerID, externalID string) (wager.Snapshot, error) {
	return s.read(ctx, func(ctx context.Context, txs app.TransactionRepository) (*wager.Transaction, error) {
		return txs.FindByExternalID(ctx, providerID, externalID)
	})
}

func (s *Service) read(ctx context.Context, find func(context.Context, app.TransactionRepository) (*wager.Transaction, error)) (wager.Snapshot, error) {
	var snap wager.Snapshot
	err := s.uow.Snapshot(ctx, func(ctx context.Context, r app.Repos) error {
		tx, err := find(ctx, r.Transactions())
		if err != nil {
			return err
		}
		snap = tx.Snapshot()
		return nil
	})
	if errors.Is(err, app.ErrNotFound) {
		return wager.Snapshot{}, fmt.Errorf("%w: %w", ErrTransactionNotFound, err)
	}
	if err != nil {
		return wager.Snapshot{}, err
	}
	return snap, nil
}
