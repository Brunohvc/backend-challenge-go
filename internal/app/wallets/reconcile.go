package wallets

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
)

// Reconciliation compares the stored balance of a wallet with the one its ledger adds up to.
type Reconciliation struct {
	WalletID       uuid.UUID
	Stored         money.Money
	Calculated     money.Money
	Difference     money.Money
	Consistent     bool
	CheckedEntries int64
}

// Reconcile compares the stored balance of the wallet with id against its ledger.
func (s *Service) Reconcile(ctx context.Context, id uuid.UUID) (Reconciliation, error) {
	var rec Reconciliation
	err := s.uow.Snapshot(ctx, func(ctx context.Context, r app.Repos) error {
		w, err := s.wallet(ctx, r, id)
		if err != nil {
			return err
		}
		totals, err := r.Ledger().Totals(ctx, id)
		if err != nil {
			return err
		}
		rec, err = reconcile(id, w.Balance(), totals)
		return err
	})
	if err != nil {
		return Reconciliation{}, err
	}
	if !rec.Consistent {
		s.metrics.ReconciliationDivergence()
		s.log.ErrorContext(logging.With(ctx, logging.Fields{WalletID: id.String()}), "wallet balance does not match its ledger",
			slog.String("currency", rec.Stored.Currency().Code()),
			slog.Int64("checkedEntries", rec.CheckedEntries))
	}
	return rec, nil
}

func reconcile(id uuid.UUID, stored money.Money, totals app.LedgerTotals) (Reconciliation, error) {
	calculated, err := money.FromMinor(totals.NetMinor, stored.Currency())
	if err != nil {
		return Reconciliation{}, fmt.Errorf("wallets: reconcile %s: %w", id, err)
	}
	difference, err := stored.Sub(calculated)
	if err != nil {
		return Reconciliation{}, fmt.Errorf("wallets: reconcile %s: %w", id, err)
	}
	return Reconciliation{
		WalletID:       id,
		Stored:         stored,
		Calculated:     calculated,
		Difference:     difference,
		Consistent:     difference.IsZero(),
		CheckedEntries: totals.Count,
	}, nil
}
