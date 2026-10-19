package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

const selectWallet = `SELECT id, player_id, currency, balance_minor, version, created_at, updated_at
  FROM wallets
 WHERE id = $1`

type walletRepo struct {
	q querier
}

var _ app.WalletRepository = walletRepo{}

func (r walletRepo) Insert(ctx context.Context, w *wallet.Wallet) error {
	s := w.Snapshot()
	_, err := r.q.Exec(ctx,
		`INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		s.ID, s.PlayerID, s.Balance.Currency().Code(), s.Balance.Minor(), s.Version, s.CreatedAt, s.UpdatedAt)
	if err != nil {
		return fmt.Errorf("postgres: insert wallet %s: %w", s.ID, classify(err))
	}
	return nil
}

func (r walletRepo) Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return r.get(ctx, selectWallet, id)
}

func (r walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return r.get(ctx, selectWallet+" FOR NO KEY UPDATE", id)
}

func (r walletRepo) get(ctx context.Context, query string, id uuid.UUID) (*wallet.Wallet, error) {
	var (
		s        wallet.Snapshot
		currency string
		balance  int64
	)
	err := r.q.QueryRow(ctx, query, id).
		Scan(&s.ID, &s.PlayerID, &currency, &balance, &s.Version, &s.CreatedAt, &s.UpdatedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, fmt.Errorf("postgres: wallet %s: %w", id, app.ErrNotFound)
	case err != nil:
		return nil, fmt.Errorf("postgres: get wallet %s: %w", id, classify(err))
	}

	if s.Balance, err = moneyOf(balance, currency); err != nil {
		return nil, unloadable("wallet", id, err)
	}
	s.CreatedAt, s.UpdatedAt = s.CreatedAt.UTC(), s.UpdatedAt.UTC()
	w, err := wallet.Rehydrate(s)
	if err != nil {
		return nil, unloadable("wallet", id, err)
	}
	return w, nil
}

func (r walletRepo) IDByPlayerAndCurrency(ctx context.Context, player uuid.UUID, currency money.Currency) (uuid.UUID, error) {
	var id uuid.UUID
	err := r.q.QueryRow(ctx,
		`SELECT id FROM wallets WHERE player_id = $1 AND currency = $2`,
		player, currency.Code()).Scan(&id)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return uuid.Nil, fmt.Errorf("postgres: wallet of player %s in %s: %w", player, currency.Code(), app.ErrNotFound)
	case err != nil:
		return uuid.Nil, fmt.Errorf("postgres: get wallet of player %s in %s: %w", player, currency.Code(), classify(err))
	}
	return id, nil
}

func (r walletRepo) UpdateBalance(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error {
	s := w.Snapshot()
	tag, err := r.q.Exec(ctx,
		`UPDATE wallets
		    SET balance_minor = $2, version = $3, updated_at = $4
		  WHERE id = $1 AND version = $5`,
		s.ID, s.Balance.Minor(), s.Version, s.UpdatedAt, expectedVersion)
	if err != nil {
		return fmt.Errorf("postgres: update wallet %s: %w", s.ID, classify(err))
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres: update wallet %s: %w", s.ID,
			app.Retryable(app.RetryVersion, fmt.Errorf("it is no longer at version %d", expectedVersion)))
	}
	return nil
}

func moneyOf(minor int64, code string) (money.Money, error) {
	c, err := money.ParseCurrency(code)
	if err != nil {
		return money.Money{}, fmt.Errorf("currency %q: %w", code, err)
	}
	return money.FromMinor(minor, c)
}

func unloadable(what string, id uuid.UUID, err error) error {
	return fmt.Errorf("postgres: %s %s cannot be loaded: %w: %w", what, id, app.ErrPermanent, err)
}
