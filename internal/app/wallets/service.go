// Package wallets is the use case of the back office: opening, reading and reconciling wallets.
package wallets

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
)

// The errors of the service, matched with errors.Is.
var (
	ErrWalletAlreadyExists = errors.New("wallets: wallet already exists")
	ErrWalletNotFound      = errors.New("wallets: wallet not found")
	ErrInvalidLedgerQuery  = errors.New("wallets: invalid ledger query")
)

// AlreadyExistsError is the error Open returns when the player already has such a wallet.
type AlreadyExistsError struct {
	WalletID uuid.UUID
	cause    error
}

func (e *AlreadyExistsError) Error() string {
	msg := fmt.Sprintf("%s: wallet %s", ErrWalletAlreadyExists.Error(), e.WalletID)
	if e.cause == nil {
		return msg
	}
	return msg + ": " + e.cause.Error()
}

// Is makes the error match ErrWalletAlreadyExists, whatever its cause.
func (e *AlreadyExistsError) Is(target error) bool { return target == ErrWalletAlreadyExists }

// Unwrap returns the error the repository reported.
func (e *AlreadyExistsError) Unwrap() error { return e.cause }

// Metrics records what the service observes.
type Metrics interface {
	ReconciliationDivergence()
	TxRetry(reason string)
}

// Service runs the wallet use cases. It is safe for concurrent use.
type Service struct {
	uow      app.UnitOfWork
	clock    app.Clock
	ids      app.IDGenerator
	metrics  Metrics
	log      *slog.Logger
	txPolicy retry.Policy
}

// NewService returns a Service whose units of work retry according to txPolicy.
func NewService(uow app.UnitOfWork, clock app.Clock, ids app.IDGenerator, m Metrics, log *slog.Logger, txPolicy retry.Policy) *Service {
	return &Service{uow: uow, clock: clock, ids: ids, metrics: m, log: log, txPolicy: txPolicy}
}

// OpenCommand asks for a new wallet.
type OpenCommand struct {
	PlayerID      uuid.UUID
	Initial       money.Money
	CorrelationID string
}

// Open creates a wallet, recording a positive initial balance as an OPENING transaction.
func (s *Service) Open(ctx context.Context, cmd OpenCommand) (wallet.Snapshot, error) {
	var opened wallet.Snapshot
	err := s.inTx(ctx, func(ctx context.Context, r app.Repos) error {
		now := s.clock.Now()
		walletID, txID := s.ids.New(), s.ids.New()
		w, entry, err := wallet.Open(wallet.OpenParams{
			ID:             walletID,
			PlayerID:       cmd.PlayerID,
			Initial:        cmd.Initial,
			OpeningTxID:    txID,
			OpeningEntryID: s.ids.New(),
			Now:            now,
		})
		if err != nil {
			return fmt.Errorf("wallets: open: %w", err)
		}
		if err := r.Wallets().Insert(ctx, w); err != nil {
			return err
		}
		if entry != nil {
			if err := s.recordOpening(ctx, r, w, *entry, txID, cmd.CorrelationID, now); err != nil {
				return err
			}
		}
		opened = w.Snapshot()
		return nil
	})
	if errors.Is(err, app.ErrAlreadyExists) {
		return wallet.Snapshot{}, s.alreadyExists(ctx, cmd.PlayerID, cmd.Initial.Currency(), err)
	}
	if err != nil {
		return wallet.Snapshot{}, err
	}
	return opened, nil
}

func (s *Service) alreadyExists(ctx context.Context, player uuid.UUID, currency money.Currency, cause error) error {
	var id uuid.UUID
	err := s.uow.Snapshot(ctx, func(ctx context.Context, r app.Repos) error {
		var err error
		id, err = r.Wallets().IDByPlayerAndCurrency(ctx, player, currency)
		return err
	})
	if err != nil {
		return fmt.Errorf("wallets: find the wallet in the way of an opening: %w", err)
	}
	return &AlreadyExistsError{WalletID: id, cause: cause}
}

func (s *Service) recordOpening(ctx context.Context, r app.Repos, w *wallet.Wallet, entry wallet.LedgerEntry, txID uuid.UUID, correlationID string, now time.Time) error {
	tx, err := wager.NewOpening(txID, w.ID(), w.PlayerID(), entry.Amount(), correlationID, now)
	if err != nil {
		return fmt.Errorf("wallets: opening transaction: %w", err)
	}
	if err := tx.MarkProcessed(wager.Result{BalanceAfter: w.Balance(), WalletVersion: w.Version()}, now); err != nil {
		return fmt.Errorf("wallets: opening transaction: %w", err)
	}
	if err := r.Transactions().Insert(ctx, tx); err != nil {
		return err
	}
	if err := r.Ledger().Append(ctx, entry); err != nil {
		return err
	}
	recs, err := s.openingEvents(tx, w, entry, correlationID, now)
	if err != nil {
		return err
	}
	return r.Outbox().Append(ctx, recs...)
}

// Get returns the wallet with id, or ErrWalletNotFound.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (wallet.Snapshot, error) {
	var snap wallet.Snapshot
	err := s.uow.Snapshot(ctx, func(ctx context.Context, r app.Repos) error {
		w, err := s.wallet(ctx, r, id)
		if err != nil {
			return err
		}
		snap = w.Snapshot()
		return nil
	})
	return snap, err
}

// LedgerPage is one page of a wallet's ledger.
type LedgerPage struct {
	Records      []app.LedgerRecord
	NextAfterSeq *int64
}

// Ledger returns at most limit entries of the wallet with id after afterSeq, oldest first.
func (s *Service) Ledger(ctx context.Context, id uuid.UUID, afterSeq int64, limit int) (LedgerPage, error) {
	if afterSeq < 0 || limit < 1 {
		return LedgerPage{}, fmt.Errorf("%w: after %d, limit %d", ErrInvalidLedgerQuery, afterSeq, limit)
	}
	var page LedgerPage
	err := s.uow.Snapshot(ctx, func(ctx context.Context, r app.Repos) error {
		if _, err := s.wallet(ctx, r, id); err != nil {
			return err
		}
		records, err := r.Ledger().Page(ctx, id, afterSeq, limit+1)
		if err != nil {
			return err
		}
		if len(records) > limit {
			records = records[:limit]
			next := records[limit-1].Seq
			page.NextAfterSeq = &next
		}
		page.Records = records
		return nil
	})
	return page, err
}

func (s *Service) wallet(ctx context.Context, r app.Repos, id uuid.UUID) (*wallet.Wallet, error) {
	w, err := r.Wallets().Get(ctx, id)
	if errors.Is(err, app.ErrNotFound) {
		return nil, fmt.Errorf("%w: %w", ErrWalletNotFound, err)
	}
	return w, err
}

func (s *Service) inTx(ctx context.Context, fn func(context.Context, app.Repos) error) error {
	onRetry := func(err error) { s.metrics.TxRetry(app.RetryReasonOf(err)) }
	err := retry.Do(ctx, s.txPolicy, isRetryable, onRetry, func(ctx context.Context) error {
		return s.uow.Do(ctx, fn)
	})
	if isRetryable(err) {
		return fmt.Errorf("wallets: %w: contention outlasted the retries: %w", app.ErrUnavailable, err)
	}
	return err
}

func isRetryable(err error) bool { return errors.Is(err, app.ErrRetryable) }
