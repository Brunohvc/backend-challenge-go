package wagering

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/failpoint"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/retry"
)

const (
	duplicateByIdempotency = "idempotency"
	duplicateByInbox       = "inbox"
)

const outcomeReplay = "REPLAY"

// Submit decides cmd and stores the outcome, ledger entry and events in one unit of work.
func (s *Service) Submit(ctx context.Context, cmd wager.Command, src Source) (res Result, err error) {
	ctx, span := s.tracer.Child(ctx, spanSubmit)
	panicked := true
	defer func() { endSubmit(span, cmd, src, res, err, panicked) }()
	res, err = s.submit(ctx, cmd, src)
	panicked = false
	return res, err
}

func (s *Service) submit(ctx context.Context, cmd wager.Command, src Source) (Result, error) {
	started := time.Now()
	ctx = logging.With(ctx, logging.Fields{WalletID: cmd.WalletID.String()})
	hash := cmd.PayloadHash()

	if src.Inbox == nil {
		stored, err := s.storedUnderKey(ctx, cmd, hash)
		if err != nil {
			return Result{}, err
		}
		if stored != nil {
			s.metrics.Duplicate(string(src.Channel), duplicateByIdempotency)
			return Result{Transaction: stored.Snapshot(), Replay: true}, nil
		}
	}

	correlationID := src.CorrelationID
	if correlationID == "" {
		correlationID = s.ids.New().String()
	}
	var res Result
	err := s.inTx(ctx, "submit", func(ctx context.Context, r app.Repos) error {
		var err error
		if src.Inbox != nil {
			res, err = s.receive(ctx, r, cmd, hash, correlationID, src)
		} else {
			res, err = s.decide(ctx, r, cmd, hash, correlationID, src.CausationID)
		}
		if err != nil {
			return err
		}
		failpoint.Inject(failpoint.SubmitBeforeCommit)
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	failpoint.Inject(failpoint.SubmitAfterCommit)

	switch {
	case res.DuplicateMessage:
		s.metrics.Duplicate(string(src.Channel), duplicateByInbox)
	case res.Replay:
		s.metrics.Duplicate(string(src.Channel), duplicateByIdempotency)
	default:
		tx := res.Transaction
		s.metrics.Transaction(string(src.Channel), string(tx.Kind), string(tx.Status), string(tx.FailureCode), time.Since(started))
	}
	return res, nil
}

func (s *Service) storedUnderKey(ctx context.Context, cmd wager.Command, hash string) (*wager.Transaction, error) {
	var stored *wager.Transaction
	err := s.uow.Snapshot(ctx, func(ctx context.Context, r app.Repos) error {
		var err error
		stored, err = replayOf(ctx, r.Transactions(), cmd, hash)
		return err
	})
	return stored, err
}

func (s *Service) receive(ctx context.Context, r app.Repos, cmd wager.Command, hash, correlationID string, src Source) (Result, error) {
	msg := src.Inbox
	claim, err := r.Inbox().Claim(ctx, msg.Consumer, msg.MessageID, msg.Hash, s.clock.Now())
	if err != nil {
		return Result{}, err
	}
	if !claim.New {
		return duplicateOf(ctx, r.Transactions(), *msg, claim)
	}

	res, err := s.decide(ctx, r, cmd, hash, correlationID, src.CausationID)
	if err != nil {
		return Result{}, err
	}
	outcome := string(res.Transaction.Status)
	if res.Replay {
		outcome = outcomeReplay
	}
	txID := uuid.NullUUID{UUID: res.Transaction.ID, Valid: true}
	if err := r.Inbox().Complete(ctx, msg.Consumer, msg.MessageID, txID, outcome, s.clock.Now()); err != nil {
		return Result{}, err
	}
	return res, nil
}

func duplicateOf(ctx context.Context, txs app.TransactionRepository, msg InboxMessage, claim app.InboxClaim) (Result, error) {
	if claim.ExistingHash != msg.Hash {
		return Result{}, fmt.Errorf("%w: message %q of %s", ErrMessageIDReused, msg.MessageID, msg.Consumer)
	}
	if !claim.TransactionID.Valid {
		return Result{DuplicateMessage: true}, nil
	}
	tx, err := txs.Get(ctx, claim.TransactionID.UUID)
	if err != nil {
		return Result{}, err
	}
	return Result{Transaction: tx.Snapshot(), DuplicateMessage: true}, nil
}

func (s *Service) decide(ctx context.Context, r app.Repos, cmd wager.Command, hash, correlationID, causationID string) (Result, error) {
	txs := r.Transactions()
	w, err := r.Wallets().GetForUpdate(ctx, cmd.WalletID)
	if errors.Is(err, app.ErrNotFound) {
		return Result{}, fmt.Errorf("%w: %w", ErrWalletNotFound, err)
	}
	if err != nil {
		return Result{}, err
	}

	stored, err := replayOf(ctx, txs, cmd, hash)
	if err != nil {
		return Result{}, err
	}
	if stored != nil {
		return Result{Transaction: stored.Snapshot(), Replay: true}, nil
	}
	if err := requireNewExternalID(ctx, txs, cmd); err != nil {
		return Result{}, err
	}
	ref, reversed, err := referenceOf(ctx, txs, cmd.ProviderID, cmd.ReferenceExternalTransactionID)
	if err != nil {
		return Result{}, err
	}

	now := s.clock.Now()
	tx, err := wager.NewExternal(s.ids.New(), cmd, correlationID, now)
	if err != nil {
		return Result{}, fmt.Errorf("wagering: submit: %w: %w", app.ErrPermanent, err)
	}
	err = s.settle(ctx, r, settlement{
		tx: tx, wallet: w, ref: ref, reversed: reversed, causationID: causationID, now: now,
	}, txs.Insert)
	if err != nil {
		return Result{}, err
	}
	return Result{Transaction: tx.Snapshot()}, nil
}

type settlement struct {
	tx          *wager.Transaction
	wallet      *wallet.Wallet
	ref         *wager.Transaction
	reversed    bool
	causationID string
	now         time.Time
}

func (s *Service) settle(ctx context.Context, r app.Repos, st settlement, save func(context.Context, *wager.Transaction) error) error {
	tx, w := st.tx, st.wallet
	readVersion := w.Version()
	decision, err := wager.Decide(wager.DecisionInput{
		Tx:                tx,
		Wallet:            w,
		Reference:         st.ref,
		ReferenceReversed: st.reversed,
		Policy:            s.cfg.Reference,
		EntryID:           s.ids.New(),
		Now:               st.now,
	})
	if err != nil {
		return fmt.Errorf("wagering: settle transaction %s: %w: %w", tx.ID(), app.ErrPermanent, err)
	}

	if err := save(ctx, tx); err != nil {
		return err
	}
	if decision.Entry != nil {
		if err := r.Ledger().Append(ctx, *decision.Entry); err != nil {
			return err
		}
		if err := r.Wallets().UpdateBalance(ctx, w, readVersion); err != nil {
			return err
		}
	}
	recs, err := outboxRecords(s.ids, tx, decision, st.causationID, st.now)
	if err != nil {
		return err
	}
	return r.Outbox().Append(ctx, recs...)
}

func replayOf(ctx context.Context, txs app.TransactionRepository, cmd wager.Command, hash string) (*wager.Transaction, error) {
	stored, err := txs.FindByIdempotencyKey(ctx, cmd.ProviderID, cmd.IdempotencyKey)
	switch {
	case errors.Is(err, app.ErrNotFound):
		return nil, nil
	case err != nil:
		return nil, err
	case stored.PayloadHash() != hash:
		return nil, fmt.Errorf("%w: the key belongs to transaction %s", ErrIdempotencyKeyReused, stored.ID())
	}
	return stored, nil
}

func requireNewExternalID(ctx context.Context, txs app.TransactionRepository, cmd wager.Command) error {
	stored, err := txs.FindByExternalID(ctx, cmd.ProviderID, cmd.ExternalTransactionID)
	switch {
	case errors.Is(err, app.ErrNotFound):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("%w: the external id belongs to transaction %s", ErrDuplicateExternalTransaction, stored.ID())
}

func referenceOf(ctx context.Context, txs app.TransactionRepository, providerID, referenceID string) (*wager.Transaction, bool, error) {
	if referenceID == "" {
		return nil, false, nil
	}
	ref, err := txs.FindByExternalID(ctx, providerID, referenceID)
	switch {
	case errors.Is(err, app.ErrNotFound):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	}
	reversed, err := txs.HasProcessedReversal(ctx, ref.ID())
	if err != nil {
		return nil, false, err
	}
	return ref, reversed, nil
}

func (s *Service) inTx(ctx context.Context, op string, fn func(context.Context, app.Repos) error) error {
	err := retry.Do(ctx, s.cfg.Tx, isRetryable, s.countRetry(ctx), func(ctx context.Context) error {
		return s.uow.Do(ctx, fn)
	})
	if isRetryable(err) {
		return fmt.Errorf("wagering: %s: %w: still conflicting with other writers: %w", op, app.ErrUnavailable, err)
	}
	return err
}

func (s *Service) countRetry(ctx context.Context) func(error) {
	return func(err error) {
		reason := app.RetryReasonOf(err)
		s.metrics.TxRetry(reason)
		s.log.DebugContext(ctx, "running a unit of work again after a conflict", slog.String("reason", reason))
	}
}

func isRetryable(err error) bool { return errors.Is(err, app.ErrRetryable) }
