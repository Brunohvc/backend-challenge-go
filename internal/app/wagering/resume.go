package wagering

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
)

const resolverChannel = "resolver"

// ResumeOutcome is how a resumption ended.
type ResumeOutcome string

// The outcomes of Resume.
const (
	ResumeProcessed   ResumeOutcome = "processed"
	ResumeRejected    ResumeOutcome = "rejected"
	ResumeRescheduled ResumeOutcome = "rescheduled"
	ResumeFailed      ResumeOutcome = "failed"
	ResumeSkipped     ResumeOutcome = "skipped"
)

// ClaimDue leases at most limit of the transactions whose next attempt is due.
func (s *Service) ClaimDue(ctx context.Context, lease time.Duration, limit int) ([]app.DueTransaction, error) {
	var due []app.DueTransaction
	err := s.uow.Do(ctx, func(ctx context.Context, r app.Repos) error {
		var err error
		due, err = r.Transactions().ClaimDue(ctx, lease, limit)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("wagering: claim due transactions: %w", err)
	}
	return due, nil
}

// Resume settles a leased transaction now that its reference may have arrived.
func (s *Service) Resume(ctx context.Context, due app.DueTransaction) (outcome ResumeOutcome, err error) {
	ctx, span := s.tracer.Child(ctx, spanResume)
	var tx *wager.Transaction
	panicked := true
	defer func() { endResume(span, due, tx, outcome, err, panicked) }()

	started := time.Now()
	ctx = logging.With(ctx, logging.Fields{TransactionID: due.ID.String(), WalletID: due.WalletID.String()})

	err = s.inTx(ctx, "resume transaction "+due.ID.String(), func(ctx context.Context, r app.Repos) error {
		var err error
		tx, outcome, err = s.resolve(ctx, r, due)
		return err
	})
	switch {
	case errors.Is(err, app.ErrPermanent):
		err = fmt.Errorf("wagering: resume transaction %s: %w", due.ID, err)
		outcome, err = s.notePermanentFailure(ctx, due, started, err)
	case err != nil:
		outcome = ""
	case outcome == ResumeProcessed || outcome == ResumeRejected:
		s.countResolution(tx, started)
	}
	panicked = false
	return outcome, err
}

func (s *Service) resolve(ctx context.Context, r app.Repos, due app.DueTransaction) (*wager.Transaction, ResumeOutcome, error) {
	w, tx, err := s.leased(ctx, r, due)
	if err != nil {
		return nil, "", err
	}
	if tx == nil {
		return nil, ResumeSkipped, nil
	}
	txs := r.Transactions()
	ref, reversed, err := referenceOf(ctx, txs, tx.ProviderID(), tx.ReferenceExternalTransactionID())
	if err != nil {
		return nil, "", err
	}

	err = s.settle(ctx, r, settlement{
		tx: tx, wallet: w, ref: ref, reversed: reversed, causationID: tx.ID().String(), now: s.clock.Now(),
	}, txs.Update)
	if err != nil {
		return nil, "", err
	}
	switch tx.Status() {
	case wager.StatusProcessed:
		return tx, ResumeProcessed, nil
	case wager.StatusRejected:
		return tx, ResumeRejected, nil
	default:
		return tx, ResumeRescheduled, nil
	}
}

func (s *Service) leased(ctx context.Context, r app.Repos, due app.DueTransaction) (*wallet.Wallet, *wager.Transaction, error) {
	w, err := r.Wallets().GetForUpdate(ctx, due.WalletID)
	if err != nil {
		return nil, nil, err
	}
	tx, err := r.Transactions().GetForUpdate(ctx, due.ID)
	if err != nil {
		return nil, nil, err
	}
	if tx.WalletID() != due.WalletID {
		s.log.DebugContext(ctx, "leaving a transaction that is not of the wallet the claim names",
			slog.String("transactionWalletId", tx.WalletID().String()))
		return nil, nil, nil
	}
	status, next := tx.Status(), tx.NextAttemptAt()
	inProgress := status == wager.StatusPending || status == wager.StatusPendingReference
	if !inProgress || next == nil || !next.Equal(due.LeaseUntil) {
		s.log.DebugContext(ctx, "leaving a transaction that is no longer leased to this claim",
			slog.String("status", string(status)), slog.Time("leaseUntil", due.LeaseUntil))
		return nil, nil, nil
	}
	return w, tx, nil
}

func (s *Service) notePermanentFailure(ctx context.Context, due app.DueTransaction, started time.Time, cause error) (ResumeOutcome, error) {
	var failed *wager.Transaction
	op := "count the permanent failure of transaction " + due.ID.String()
	err := s.inTx(ctx, op, func(ctx context.Context, r app.Repos) error {
		failed = nil
		_, tx, err := s.leased(ctx, r, due)
		if err != nil || tx == nil {
			return err
		}
		if err := tx.NotePermanentFailure(s.cfg.PermanentFailureLimit, s.clock.Now()); err != nil {
			return fmt.Errorf("wagering: count a permanent failure of transaction %s: %w: %w", tx.ID(), app.ErrPermanent, err)
		}
		if err := r.Transactions().Update(ctx, tx); err != nil {
			return err
		}
		if tx.Status() == wager.StatusFailed {
			failed = tx
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("%w (the failure could not be counted: %w)", cause, err)
	}
	if failed == nil {
		return "", cause
	}
	ctx = logging.With(ctx, logging.Fields{CorrelationID: failed.CorrelationID(), ProviderID: failed.ProviderID()})
	s.log.ErrorContext(ctx, "transaction failed after repeated permanent errors",
		slog.Int("permanentFailures", failed.PermanentFailures()), slog.String("error", cause.Error()))
	s.countResolution(failed, started)
	return ResumeFailed, cause
}

func (s *Service) countResolution(tx *wager.Transaction, started time.Time) {
	s.metrics.Transaction(resolverChannel, string(tx.Kind()), string(tx.Status()), string(tx.FailureCode()), time.Since(started))
}
