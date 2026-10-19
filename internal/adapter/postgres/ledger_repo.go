package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

type ledgerRepo struct {
	q querier
}

var _ app.LedgerRepository = ledgerRepo{}

func (r ledgerRepo) Append(ctx context.Context, e wallet.LedgerEntry) error {
	_, err := r.q.Exec(ctx,
		`INSERT INTO wallet_ledger_entries
		        (id, wallet_id, transaction_id, direction, amount_minor, currency,
		         balance_before_minor, balance_after_minor, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Amount().Minor(),
		e.Amount().Currency().Code(), e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.CreatedAt())
	if err != nil {
		return fmt.Errorf("postgres: append ledger entry %s: %w", e.ID(), classify(err))
	}
	return nil
}

func (r ledgerRepo) Page(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]app.LedgerRecord, error) {
	if limit < 1 {
		return nil, fmt.Errorf("postgres: ledger of wallet %s: %w: page limit %d is not positive",
			walletID, app.ErrPermanent, limit)
	}
	rows, err := r.q.Query(ctx,
		`SELECT seq, id, wallet_id, transaction_id, direction, amount_minor, currency,
		        balance_before_minor, balance_after_minor, created_at
		   FROM wallet_ledger_entries
		  WHERE wallet_id = $1 AND seq > $2
		  ORDER BY seq
		  LIMIT $3`,
		walletID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: ledger of wallet %s: %w", walletID, classify(err))
	}
	records, err := pgx.CollectRows(rows, scanLedgerRecord)
	if err != nil {
		return nil, fmt.Errorf("postgres: ledger of wallet %s: %w", walletID, classify(err))
	}
	return records, nil
}

func scanLedgerRecord(row pgx.CollectableRow) (app.LedgerRecord, error) {
	var (
		seq                   int64
		id, walletID, txID    uuid.UUID
		direction, currency   string
		amount, before, after int64
		createdAt             time.Time
	)
	if err := row.Scan(&seq, &id, &walletID, &txID, &direction, &amount, &currency, &before, &after, &createdAt); err != nil {
		return app.LedgerRecord{}, err
	}
	amountM, err := moneyOf(amount, currency)
	if err != nil {
		return app.LedgerRecord{}, unloadable("ledger entry", id, err)
	}
	beforeM, err := moneyOf(before, currency)
	if err != nil {
		return app.LedgerRecord{}, unloadable("ledger entry", id, err)
	}
	afterM, err := moneyOf(after, currency)
	if err != nil {
		return app.LedgerRecord{}, unloadable("ledger entry", id, err)
	}
	entry, err := wallet.NewLedgerEntry(id, walletID, txID, wallet.Direction(direction),
		amountM, beforeM, afterM, createdAt.UTC())
	if err != nil {
		return app.LedgerRecord{}, unloadable("ledger entry", id, err)
	}
	return app.LedgerRecord{Seq: seq, Entry: entry}, nil
}

func (r ledgerRepo) Totals(ctx context.Context, walletID uuid.UUID) (app.LedgerTotals, error) {
	var t app.LedgerTotals
	err := r.q.QueryRow(ctx,
		`SELECT (COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'CREDIT'), 0)
		         - COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'DEBIT'), 0))::bigint,
		        count(*)
		   FROM wallet_ledger_entries
		  WHERE wallet_id = $1`,
		walletID).Scan(&t.NetMinor, &t.Count)
	if err != nil {
		return app.LedgerTotals{}, fmt.Errorf("postgres: ledger totals of wallet %s: %w", walletID, classify(err))
	}
	return t, nil
}
