package wagering

import (
	"fmt"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/event"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

func outboxRecords(ids app.IDGenerator, tx *wager.Transaction, d wager.Decision, causationID string, at time.Time) ([]app.OutboxRecord, error) {
	var builds []func(event.Meta) (event.Envelope, error)
	switch {
	case tx.Status() == wager.StatusProcessed:
		builds = append(builds, func(m event.Meta) (event.Envelope, error) {
			return event.NewWagerTransactionProcessed(m, processedData(tx))
		})
		if entry := d.Entry; entry != nil {
			builds = append(builds, func(m event.Meta) (event.Envelope, error) {
				return event.NewWalletBalanceChanged(m, balanceChangedData(*entry, tx.ResultWalletVersion()))
			})
		}
	case tx.Status() == wager.StatusRejected:
		builds = append(builds, func(m event.Meta) (event.Envelope, error) {
			return event.NewWagerTransactionRejected(m, rejectedData(tx))
		})
	case d.EnteredPendingReference:
		builds = append(builds, func(m event.Meta) (event.Envelope, error) {
			return event.NewWagerTransactionPendingReference(m, pendingReferenceData(tx))
		})
	}

	recs := make([]app.OutboxRecord, 0, len(builds))
	for _, build := range builds {
		env, err := build(event.Meta{
			EventID:       ids.New(),
			WalletID:      tx.WalletID(),
			CorrelationID: tx.CorrelationID(),
			CausationID:   causationID,
			OccurredAt:    at,
		})
		if err != nil {
			return nil, fmt.Errorf("wagering: event of transaction %s: %w: %w", tx.ID(), app.ErrPermanent, err)
		}
		rec, err := app.NewOutboxRecord(env)
		if err != nil {
			return nil, fmt.Errorf("wagering: event of transaction %s: %w", tx.ID(), err)
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

func processedData(tx *wager.Transaction) event.WagerTransactionProcessedData {
	d := event.WagerTransactionProcessedData{
		TransactionID:         tx.ID().String(),
		WalletID:              tx.WalletID().String(),
		PlayerID:              tx.PlayerID().String(),
		Kind:                  string(tx.Kind()),
		Money:                 tx.Money(),
		BalanceAfter:          *tx.ResultBalance(),
		WalletVersion:         tx.ResultWalletVersion(),
		ProviderID:            tx.ProviderID(),
		ExternalTransactionID: tx.ExternalTransactionID(),
		RoundID:               tx.RoundID(),
		GameID:                tx.GameID(),
	}
	if ref := tx.ReferenceTransactionID(); ref.Valid {
		d.ReferenceTransactionID = ref.UUID.String()
	}
	return d
}

func balanceChangedData(entry wallet.LedgerEntry, walletVersion int64) event.WalletBalanceChangedData {
	return event.WalletBalanceChangedData{
		WalletID:      entry.WalletID().String(),
		TransactionID: entry.TransactionID().String(),
		Direction:     string(entry.Direction()),
		Money:         entry.Amount(),
		BalanceBefore: entry.BalanceBefore(),
		BalanceAfter:  entry.BalanceAfter(),
		WalletVersion: walletVersion,
	}
}

func rejectedData(tx *wager.Transaction) event.WagerTransactionRejectedData {
	return event.WagerTransactionRejectedData{
		TransactionID:         tx.ID().String(),
		ProviderID:            tx.ProviderID(),
		ExternalTransactionID: tx.ExternalTransactionID(),
		WalletID:              tx.WalletID().String(),
		Kind:                  string(tx.Kind()),
		Money:                 tx.Money(),
		FailureCode:           string(tx.FailureCode()),
	}
}

func pendingReferenceData(tx *wager.Transaction) event.WagerTransactionPendingReferenceData {
	return event.WagerTransactionPendingReferenceData{
		TransactionID:                  tx.ID().String(),
		ProviderID:                     tx.ProviderID(),
		ExternalTransactionID:          tx.ExternalTransactionID(),
		WalletID:                       tx.WalletID().String(),
		Kind:                           string(tx.Kind()),
		ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID(),
		NextAttemptAt:                  event.Timestamp(*tx.NextAttemptAt()),
		ExpiresAt:                      event.Timestamp(*tx.ExpiresAt()),
	}
}
