package wallets

import (
	"fmt"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

func (s *Service) openingEvents(tx *wager.Transaction, w *wallet.Wallet, entry wallet.LedgerEntry, correlationID string, now time.Time) ([]app.OutboxRecord, error) {
	meta := func() event.Meta {
		return event.Meta{EventID: s.ids.New(), WalletID: w.ID(), CorrelationID: correlationID, OccurredAt: now}
	}
	processed, err := event.NewWagerTransactionProcessed(meta(), event.WagerTransactionProcessedData{
		TransactionID: tx.ID().String(),
		WalletID:      w.ID().String(),
		PlayerID:      w.PlayerID().String(),
		Kind:          string(tx.Kind()),
		Money:         tx.Money(),
		BalanceAfter:  w.Balance(),
		WalletVersion: w.Version(),
	})
	if err != nil {
		return nil, fmt.Errorf("wallets: opening events: %w", err)
	}
	changed, err := event.NewWalletBalanceChanged(meta(), event.WalletBalanceChangedData{
		WalletID:      w.ID().String(),
		TransactionID: tx.ID().String(),
		Direction:     string(entry.Direction()),
		Money:         entry.Amount(),
		BalanceBefore: entry.BalanceBefore(),
		BalanceAfter:  entry.BalanceAfter(),
		WalletVersion: w.Version(),
	})
	if err != nil {
		return nil, fmt.Errorf("wallets: opening events: %w", err)
	}

	recs := make([]app.OutboxRecord, 0, 2)
	for _, e := range []event.Envelope{processed, changed} {
		rec, err := app.NewOutboxRecord(e)
		if err != nil {
			return nil, fmt.Errorf("wallets: opening events: %w", err)
		}
		recs = append(recs, rec)
	}
	return recs, nil
}
