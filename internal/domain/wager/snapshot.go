package wager

import (
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
)

// Snapshot is the persistent state of a Transaction, one field per stored column.
type Snapshot struct {
	ID     uuid.UUID
	Origin Origin
	Kind   Kind
	Status Status

	WalletID uuid.UUID
	PlayerID uuid.UUID
	Money    money.Money

	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	RoundID                        string
	GameID                         string
	ReferenceExternalTransactionID string

	ReferenceTransactionID uuid.NullUUID
	FailureCode            FailureCode

	ResultBalance       *money.Money
	ResultWalletVersion int64

	CorrelationID     string
	ReferenceAttempts int
	PermanentFailures int

	NextAttemptAt *time.Time
	ExpiresAt     *time.Time

	CreatedAt   time.Time
	UpdatedAt   time.Time
	ProcessedAt *time.Time
}

func (s Snapshot) clone() Snapshot {
	s.ResultBalance = clonePtr(s.ResultBalance)
	s.NextAttemptAt = clonePtr(s.NextAttemptAt)
	s.ExpiresAt = clonePtr(s.ExpiresAt)
	s.ProcessedAt = clonePtr(s.ProcessedAt)
	return s
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// Rehydrate rebuilds a Transaction from stored state, validating the snapshot.
func Rehydrate(s Snapshot) (*Transaction, error) {
	s = s.clone()
	if err := s.validate(); err != nil {
		return nil, err
	}
	return &Transaction{state: s}, nil
}

// Snapshot returns the persistent state of the transaction, as a copy.
func (t *Transaction) Snapshot() Snapshot {
	return t.state.clone()
}

func (s Snapshot) validate() error {
	for _, check := range [...]func(Snapshot) error{
		Snapshot.validateIdentity,
		Snapshot.validateOrigin,
		Snapshot.validateKind,
		Snapshot.validateStatus,
		Snapshot.validateSchedule,
		Snapshot.validateResult,
		Snapshot.validateReference,
	} {
		if err := check(s); err != nil {
			return err
		}
	}
	return nil
}

func (s Snapshot) validateIdentity() error {
	switch {
	case s.ID == uuid.Nil:
		return invalidTransaction("id is nil")
	case s.WalletID == uuid.Nil:
		return invalidTransaction("wallet id is nil")
	case s.PlayerID == uuid.Nil:
		return invalidTransaction("player id is nil")
	case s.CorrelationID == "":
		return invalidTransaction("correlation id is empty")
	case s.CreatedAt.IsZero():
		return invalidTransaction("created at is zero")
	case s.UpdatedAt.IsZero():
		return invalidTransaction("updated at is zero")
	case s.UpdatedAt.Before(s.CreatedAt):
		return invalidTransaction("updated at is before created at")
	case s.ReferenceAttempts < 0 || s.PermanentFailures < 0:
		return invalidTransaction("attempt counters cannot be negative")
	}
	return nil
}

func (s Snapshot) validateOrigin() error {
	switch s.Origin {
	case OriginInternal:
		if s.Kind != KindOpening {
			return invalidTransaction("an internal transaction must be an OPENING")
		}
		if s.ProviderID != "" || s.ExternalTransactionID != "" || s.IdempotencyKey != "" ||
			s.PayloadHash != "" || s.RoundID != "" || s.GameID != "" || s.ReferenceExternalTransactionID != "" {
			return invalidTransaction("an internal transaction carries no provider metadata")
		}
	case OriginExternal:
		if _, ok := parseExternalKind(string(s.Kind)); !ok {
			return invalidTransaction("an external transaction must be of a kind a provider may send")
		}
		for _, f := range [...]struct{ name, value string }{
			{"provider id", s.ProviderID},
			{"external transaction id", s.ExternalTransactionID},
			{"idempotency key", s.IdempotencyKey},
			{"payload hash", s.PayloadHash},
			{"round id", s.RoundID},
			{"game id", s.GameID},
		} {
			if f.value == "" {
				return invalidTransaction("an external transaction needs its " + f.name)
			}
		}
	default:
		return invalidTransaction("unknown origin")
	}
	return nil
}

func (s Snapshot) validateKind() error {
	switch {
	case !s.Money.IsValid():
		return invalidTransaction("money has no currency")
	case s.Kind.mustBeZero() && !s.Money.IsZero():
		return invalidTransaction("the amount of a LOSS must be zero")
	case s.Kind.mustBePositive() && !s.Money.IsPositive():
		return invalidTransaction("the amount of a transaction that moves money must be positive")
	case s.Kind.requiresReference() && s.ReferenceExternalTransactionID == "":
		return invalidTransaction("a REFUND or ROLLBACK needs the reference to the transaction it acts on")
	case s.Kind.forbidsReference() && s.ReferenceExternalTransactionID != "":
		return invalidTransaction("a BET or LOSS takes no reference")
	}
	return nil
}

func (s Snapshot) validateStatus() error {
	switch s.Status {
	case StatusPending, StatusPendingReference, StatusProcessed:
		if s.FailureCode != "" {
			return invalidTransaction("only a rejected or failed transaction has a failure code")
		}
	case StatusRejected:
		if !s.FailureCode.isRejection() {
			return invalidTransaction("a rejected transaction needs one of the rejection codes")
		}
	case StatusFailed:
		if s.FailureCode != FailureProcessingFailed {
			return invalidTransaction("a failed transaction has the code PROCESSING_FAILED")
		}
	default:
		return invalidTransaction("unknown status")
	}

	switch {
	case s.Status.IsTerminal() && s.ProcessedAt == nil:
		return invalidTransaction("a final transaction needs its processed at")
	case !s.Status.IsTerminal() && s.ProcessedAt != nil:
		return invalidTransaction("a transaction still in progress has no processed at")
	case s.ProcessedAt != nil && s.ProcessedAt.IsZero():
		return invalidTransaction("processed at is zero")
	}
	return nil
}

func (s Snapshot) validateSchedule() error {
	switch {
	case s.Status == StatusPendingReference && s.ReferenceExternalTransactionID == "":
		return invalidTransaction("a transaction with no reference cannot wait for one")
	case s.Status == StatusPendingReference && (s.NextAttemptAt == nil || s.ExpiresAt == nil):
		return invalidTransaction("a transaction waiting for its reference needs the next attempt and the expiry")
	case s.NextAttemptAt != nil && s.NextAttemptAt.IsZero() || s.ExpiresAt != nil && s.ExpiresAt.IsZero():
		return invalidTransaction("next attempt at and expires at cannot be zero")
	}
	return nil
}

func (s Snapshot) validateResult() error {
	hasBalance := s.ResultBalance != nil
	switch {
	case hasBalance != (s.ResultWalletVersion != 0):
		return invalidTransaction("result balance and result wallet version go together")
	case !hasBalance && s.Status == StatusProcessed:
		return invalidTransaction("a processed transaction needs its result")
	case !hasBalance:
		return nil
	case s.Status != StatusProcessed && s.Status != StatusRejected:
		return invalidTransaction("only a processed or rejected transaction has a result")
	case s.ResultWalletVersion < 1:
		return invalidTransaction("result wallet version must be positive")
	case !s.ResultBalance.IsValid() || s.ResultBalance.IsNegative():
		return invalidTransaction("result balance must be a non-negative amount")
	case s.ResultBalance.Currency() != s.Money.Currency():
		return invalidTransaction("result balance must be in the currency of the amount")
	}
	return nil
}

func (s Snapshot) validateReference() error {
	ref := s.ReferenceTransactionID
	switch {
	case ref.Valid && (ref.UUID == uuid.Nil || ref.UUID == s.ID):
		return invalidTransaction("reference transaction id is nil or the transaction itself")
	case ref.Valid && s.ReferenceExternalTransactionID == "":
		return invalidTransaction("a transaction with no reference cannot have resolved one")
	case s.Status == StatusProcessed && s.Kind.requiresReference() && !ref.Valid:
		return invalidTransaction("a processed REFUND or ROLLBACK needs the transaction it resolved")
	}
	return nil
}
