package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

const selectTransaction = `SELECT id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
       provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
       reference_external_transaction_id, reference_transaction_id, failure_code,
       result_balance_minor, result_wallet_version, correlation_id, reference_attempts, permanent_failures,
       next_attempt_at, expires_at, created_at, updated_at, processed_at
  FROM wager_transactions
 WHERE `

const (
	selectByIdempotencyKey = selectTransaction +
		`origin = 'EXTERNAL' AND provider_id = $1 AND idempotency_key = $2`
	selectByExternalID = selectTransaction +
		`origin = 'EXTERNAL' AND provider_id = $1 AND external_transaction_id = $2`
)

const insertTransaction = `INSERT INTO wager_transactions
       (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
        provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
        reference_external_transaction_id, reference_transaction_id, failure_code,
        result_balance_minor, result_wallet_version, correlation_id, reference_attempts, permanent_failures,
        next_attempt_at, expires_at, created_at, updated_at, processed_at)
VALUES (@id, @origin, @kind, @status, @wallet_id, @player_id, @amount_minor, @currency,
        @provider_id, @external_transaction_id, @idempotency_key, @payload_hash, @round_id, @game_id,
        @reference_external_transaction_id, @reference_transaction_id, @failure_code,
        @result_balance_minor, @result_wallet_version, @correlation_id, @reference_attempts, @permanent_failures,
        @next_attempt_at, @expires_at, @created_at, @updated_at, @processed_at)`

const updateTransaction = `UPDATE wager_transactions
   SET status = @status,
       reference_transaction_id = @reference_transaction_id,
       failure_code = @failure_code,
       result_balance_minor = @result_balance_minor,
       result_wallet_version = @result_wallet_version,
       reference_attempts = @reference_attempts,
       permanent_failures = @permanent_failures,
       next_attempt_at = @next_attempt_at,
       expires_at = @expires_at,
       updated_at = @updated_at,
       processed_at = @processed_at
 WHERE id = @id`

const claimDue = `WITH due AS (
    SELECT id, next_attempt_at
      FROM wager_transactions
     WHERE status IN ('PENDING', 'PENDING_REFERENCE')
       AND next_attempt_at <= now()
     ORDER BY next_attempt_at
     LIMIT $2
       FOR NO KEY UPDATE SKIP LOCKED
), claimed AS (
    UPDATE wager_transactions t
       SET next_attempt_at = now() + $1::interval
      FROM due
     WHERE t.id = due.id
 RETURNING t.id, t.wallet_id, t.next_attempt_at AS lease_until, due.next_attempt_at AS due_at
)
SELECT id, wallet_id, lease_until FROM claimed ORDER BY due_at`

type transactionRepo struct {
	q querier
}

var _ app.TransactionRepository = transactionRepo{}

func (r repos) Transactions() app.TransactionRepository { return transactionRepo{q: r.tx} }

func (r transactionRepo) Insert(ctx context.Context, t *wager.Transaction) error {
	s := t.Snapshot()
	_, err := r.q.Exec(ctx, insertTransaction, pgx.StrictNamedArgs{
		"id":                                s.ID,
		"origin":                            string(s.Origin),
		"kind":                              string(s.Kind),
		"status":                            string(s.Status),
		"wallet_id":                         s.WalletID,
		"player_id":                         s.PlayerID,
		"amount_minor":                      s.Money.Minor(),
		"currency":                          s.Money.Currency().Code(),
		"provider_id":                       nullIfZero(s.ProviderID),
		"external_transaction_id":           nullIfZero(s.ExternalTransactionID),
		"idempotency_key":                   nullIfZero(s.IdempotencyKey),
		"payload_hash":                      nullIfZero(s.PayloadHash),
		"round_id":                          nullIfZero(s.RoundID),
		"game_id":                           nullIfZero(s.GameID),
		"reference_external_transaction_id": nullIfZero(s.ReferenceExternalTransactionID),
		"reference_transaction_id":          s.ReferenceTransactionID,
		"failure_code":                      nullIfZero(string(s.FailureCode)),
		"result_balance_minor":              resultBalanceMinor(s),
		"result_wallet_version":             nullIfZero(s.ResultWalletVersion),
		"correlation_id":                    s.CorrelationID,
		"reference_attempts":                s.ReferenceAttempts,
		"permanent_failures":                s.PermanentFailures,
		"next_attempt_at":                   s.NextAttemptAt,
		"expires_at":                        s.ExpiresAt,
		"created_at":                        s.CreatedAt,
		"updated_at":                        s.UpdatedAt,
		"processed_at":                      s.ProcessedAt,
	})
	if err != nil {
		return fmt.Errorf("postgres: insert transaction %s: %w", s.ID, classify(err))
	}
	return nil
}

func (r transactionRepo) Update(ctx context.Context, t *wager.Transaction) error {
	s := t.Snapshot()
	tag, err := r.q.Exec(ctx, updateTransaction, pgx.StrictNamedArgs{
		"id":                       s.ID,
		"status":                   string(s.Status),
		"reference_transaction_id": s.ReferenceTransactionID,
		"failure_code":             nullIfZero(string(s.FailureCode)),
		"result_balance_minor":     resultBalanceMinor(s),
		"result_wallet_version":    nullIfZero(s.ResultWalletVersion),
		"reference_attempts":       s.ReferenceAttempts,
		"permanent_failures":       s.PermanentFailures,
		"next_attempt_at":          s.NextAttemptAt,
		"expires_at":               s.ExpiresAt,
		"updated_at":               s.UpdatedAt,
		"processed_at":             s.ProcessedAt,
	})
	if err != nil {
		return fmt.Errorf("postgres: update transaction %s: %w", s.ID, classify(err))
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres: update transaction %s: %w", s.ID, app.ErrNotFound)
	}
	return nil
}

func (r transactionRepo) Get(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return r.find(ctx, "transaction "+id.String(), selectTransaction+"id = $1", id)
}

func (r transactionRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return r.find(ctx, "transaction "+id.String(), selectTransaction+"id = $1 FOR NO KEY UPDATE", id)
}

func (r transactionRepo) FindByIdempotencyKey(ctx context.Context, providerID, key string) (*wager.Transaction, error) {
	what := fmt.Sprintf("transaction of provider %q with idempotency key %q", providerID, key)
	return r.find(ctx, what, selectByIdempotencyKey, providerID, key)
}

func (r transactionRepo) FindByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	what := fmt.Sprintf("transaction of provider %q with external id %q", providerID, externalID)
	return r.find(ctx, what, selectByExternalID, providerID, externalID)
}

func (r transactionRepo) find(ctx context.Context, what, query string, args ...any) (*wager.Transaction, error) {
	var row transactionRow
	err := row.scan(r.q.QueryRow(ctx, query, args...))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, fmt.Errorf("postgres: %s: %w", what, app.ErrNotFound)
	case err != nil:
		return nil, fmt.Errorf("postgres: get %s: %w", what, classify(err))
	}
	return row.load()
}

func (r transactionRepo) HasProcessedReversal(ctx context.Context, referenceTxID uuid.UUID) (bool, error) {
	var reversed bool
	err := r.q.QueryRow(ctx,
		`SELECT EXISTS (
		        SELECT 1
		          FROM wager_transactions
		         WHERE reference_transaction_id = $1
		           AND status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK'))`,
		referenceTxID).Scan(&reversed)
	if err != nil {
		return false, fmt.Errorf("postgres: reversal of transaction %s: %w", referenceTxID, classify(err))
	}
	return reversed, nil
}

func (r transactionRepo) ClaimDue(ctx context.Context, lease time.Duration, limit int) ([]app.DueTransaction, error) {
	switch {
	case lease <= 0:
		return nil, fmt.Errorf("postgres: claim due transactions: %w: lease %s is not positive", app.ErrPermanent, lease)
	case limit < 1:
		return nil, fmt.Errorf("postgres: claim due transactions: %w: limit %d is not positive", app.ErrPermanent, limit)
	}
	rows, err := r.q.Query(ctx, claimDue, lease, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim due transactions: %w", classify(err))
	}
	due, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (app.DueTransaction, error) {
		var d app.DueTransaction
		err := row.Scan(&d.ID, &d.WalletID, &d.LeaseUntil)
		d.LeaseUntil = d.LeaseUntil.UTC()
		return d, err
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: claim due transactions: %w", classify(err))
	}
	return due, nil
}

type transactionRow struct {
	s                                       wager.Snapshot
	origin, kind, status, currency          string
	amount                                  int64
	providerID, externalID, idempotencyKey  *string
	payloadHash, roundID, gameID, reference *string
	failureCode                             *string
	resultBalance, resultWalletVersion      *int64
}

func (row *transactionRow) scan(r pgx.Row) error {
	s := &row.s
	return r.Scan(&s.ID, &row.origin, &row.kind, &row.status, &s.WalletID, &s.PlayerID, &row.amount, &row.currency,
		&row.providerID, &row.externalID, &row.idempotencyKey, &row.payloadHash, &row.roundID, &row.gameID,
		&row.reference, &s.ReferenceTransactionID, &row.failureCode,
		&row.resultBalance, &row.resultWalletVersion, &s.CorrelationID, &s.ReferenceAttempts, &s.PermanentFailures,
		&s.NextAttemptAt, &s.ExpiresAt, &s.CreatedAt, &s.UpdatedAt, &s.ProcessedAt)
}

func (row *transactionRow) load() (*wager.Transaction, error) {
	s := row.s
	s.Origin, s.Kind, s.Status = wager.Origin(row.origin), wager.Kind(row.kind), wager.Status(row.status)
	var err error
	if s.Money, err = moneyOf(row.amount, row.currency); err != nil {
		return nil, unloadable("transaction", s.ID, err)
	}
	if row.resultBalance != nil {
		var balance money.Money
		if balance, err = moneyOf(*row.resultBalance, row.currency); err != nil {
			return nil, unloadable("transaction", s.ID, err)
		}
		s.ResultBalance = &balance
	}
	s.ResultWalletVersion = zeroIfNull(row.resultWalletVersion)
	s.ProviderID = zeroIfNull(row.providerID)
	s.ExternalTransactionID = zeroIfNull(row.externalID)
	s.IdempotencyKey = zeroIfNull(row.idempotencyKey)
	s.PayloadHash = zeroIfNull(row.payloadHash)
	s.RoundID = zeroIfNull(row.roundID)
	s.GameID = zeroIfNull(row.gameID)
	s.ReferenceExternalTransactionID = zeroIfNull(row.reference)
	s.FailureCode = wager.FailureCode(zeroIfNull(row.failureCode))
	s.CreatedAt, s.UpdatedAt = s.CreatedAt.UTC(), s.UpdatedAt.UTC()
	s.NextAttemptAt, s.ExpiresAt, s.ProcessedAt = inUTC(s.NextAttemptAt), inUTC(s.ExpiresAt), inUTC(s.ProcessedAt)

	t, err := wager.Rehydrate(s)
	if err != nil {
		return nil, unloadable("transaction", s.ID, err)
	}
	return t, nil
}

func resultBalanceMinor(s wager.Snapshot) any {
	if s.ResultBalance == nil {
		return nil
	}
	return s.ResultBalance.Minor()
}

func nullIfZero[T comparable](v T) any {
	var zero T
	if v == zero {
		return nil
	}
	return v
}

func zeroIfNull[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

func inUTC(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	utc := t.UTC()
	return &utc
}
