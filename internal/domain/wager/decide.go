package wager

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
)

// DecisionInput is what Decide needs to settle a transaction.
type DecisionInput struct {
	Tx                *Transaction
	Wallet            *wallet.Wallet
	Reference         *Transaction
	ReferenceReversed bool
	Policy            ReferencePolicy
	EntryID           uuid.UUID
	Now               time.Time
}

// Decision is what Decide produces besides the new transaction and wallet state.
type Decision struct {
	Entry                   *wallet.LedgerEntry
	EnteredPendingReference bool
}

// Decide settles a transaction as PROCESSED, REJECTED or PENDING_REFERENCE.
func Decide(in DecisionInput) (Decision, error) {
	if err := in.validate(); err != nil {
		return Decision{}, err
	}
	if code := in.ownershipFailure(); code != "" {
		return Decision{}, in.Tx.Reject(code, money.Money{}, 0, in.Now)
	}
	if in.Tx.ReferenceExternalTransactionID() != "" {
		code, wait := in.referenceFailure()
		if code != "" {
			return Decision{}, in.reject(code)
		}
		if wait {
			return in.wait()
		}
	}
	return in.move()
}

func (in DecisionInput) validate() error {
	tx, ref := in.Tx, in.Reference
	switch {
	case tx == nil || in.Wallet == nil:
		return invalidDecision("the transaction and the wallet are required")
	case tx.Status().IsTerminal():
		return ErrTerminalState
	case in.Now.IsZero():
		return invalidDecision("the time of the decision is zero")
	case tx.Origin() != OriginExternal:
		return invalidDecision("only a transaction sent by a provider is decided")
	case tx.WalletID() != in.Wallet.ID():
		return invalidDecision("the wallet is not the one the transaction names")
	case ref == nil && in.ReferenceReversed:
		return invalidDecision("a reversal was reported for a reference that was not found")
	case ref != nil && tx.ReferenceExternalTransactionID() == "":
		return invalidDecision("a reference was given for a transaction that names none")
	case ref != nil && ref.ExternalTransactionID() != tx.ReferenceExternalTransactionID():
		return invalidDecision("the reference is not the transaction this one names")
	case tx.ReferenceExternalTransactionID() != "":
		return in.Policy.validate()
	}
	return nil
}

func (in DecisionInput) ownershipFailure() FailureCode {
	switch {
	case in.Wallet.PlayerID() != in.Tx.PlayerID():
		return FailureWalletPlayerMismatch
	case in.Wallet.Currency() != in.Tx.Money().Currency():
		return FailureCurrencyMismatch
	}
	return ""
}

func (in DecisionInput) referenceFailure() (code FailureCode, wait bool) {
	tx, ref := in.Tx, in.Reference
	switch {
	case ref == nil:
		return "", true
	case !tx.Kind().canReference(ref.Kind()):
		return FailureInvalidReferenceKind, false
	case !ref.Status().IsTerminal():
		return "", true
	case ref.Status() != StatusProcessed:
		return FailureReferenceNotProcessed, false
	case !sameContext(tx, ref):
		return FailureReferenceMismatch, false
	case tx.Kind() != KindWin && !ref.Money().Equal(tx.Money()):
		return FailureReversalAmountMismatch, false
	case in.ReferenceReversed:
		return FailureReferenceAlreadyReversed, false
	}
	return "", false
}

func sameContext(tx, ref *Transaction) bool {
	return ref.ProviderID() == tx.ProviderID() &&
		ref.PlayerID() == tx.PlayerID() &&
		ref.WalletID() == tx.WalletID() &&
		ref.Money().Currency() == tx.Money().Currency() &&
		ref.RoundID() == tx.RoundID()
}

func (in DecisionInput) wait() (Decision, error) {
	tx, policy, now := in.Tx, in.Policy, in.Now

	if tx.Status() == StatusPending {
		expires := now.Add(policy.TTL)
		next := earliest(policy.NextAttemptAt(0, now), expires)
		if err := tx.AwaitReference(next, expires, now); err != nil {
			return Decision{}, err
		}
		return Decision{EnteredPendingReference: true}, nil
	}

	missed, expires := tx.ReferenceAttempts(), *tx.ExpiresAt()
	attemptsExhausted := missed >= policy.MaxAttempts-1
	expired := !now.Before(expires)
	if attemptsExhausted || expired {
		code := FailureReferenceNotFound
		if in.Reference != nil {
			code = FailureReferenceNotProcessed
		}
		return Decision{}, in.reject(code)
	}
	next := earliest(policy.NextAttemptAt(missed+1, now), expires)
	return Decision{}, tx.RescheduleReference(next, now)
}

func (in DecisionInput) move() (Decision, error) {
	tx := in.Tx
	result := Result{BalanceAfter: in.Wallet.Balance(), WalletVersion: in.Wallet.Version()}
	if in.Reference != nil {
		result.ReferenceTransactionID = uuid.NullUUID{UUID: in.Reference.ID(), Valid: true}
	}
	if tx.Kind() == KindLoss {
		return Decision{}, tx.MarkProcessed(result, in.Now)
	}
	if in.EntryID == uuid.Nil {
		return Decision{}, invalidDecision("a movement of money needs the id of its ledger entry")
	}

	walletCopy := *in.Wallet
	apply, shortfall := walletCopy.Credit, FailureCode("")
	switch {
	case tx.Kind() == KindBet:
		apply, shortfall = walletCopy.Debit, FailureInsufficientFunds
	case tx.Kind() == KindRollback && in.Reference.Kind() != KindBet:
		apply, shortfall = walletCopy.Debit, FailureReversalInsufficientFunds
	}

	entry, err := apply(tx.Money(), tx.ID(), in.EntryID, in.Now)
	switch {
	case errors.Is(err, wallet.ErrInsufficientFunds):
		return Decision{}, in.reject(shortfall)
	case errors.Is(err, wallet.ErrBalanceLimit):
		return Decision{}, in.reject(FailureBalanceLimitExceeded)
	case err != nil:
		return Decision{}, fmt.Errorf("wager: moving the money of the transaction: %w", err)
	}

	result.BalanceAfter, result.WalletVersion = walletCopy.Balance(), walletCopy.Version()
	if err := tx.MarkProcessed(result, in.Now); err != nil {
		return Decision{}, err
	}
	*in.Wallet = walletCopy
	return Decision{Entry: &entry}, nil
}

func (in DecisionInput) reject(code FailureCode) error {
	return in.Tx.Reject(code, in.Wallet.Balance(), in.Wallet.Version(), in.Now)
}

func earliest(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}
