package wager

import (
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
)

// Transaction is a wagering transaction and the state machine it goes through.
type Transaction struct {
	state Snapshot
}

// NewExternal starts the PENDING transaction for a command a provider sent.
func NewExternal(id uuid.UUID, cmd Command, correlationID string, now time.Time) (*Transaction, error) {
	if err := validateIdentifiers(cmd); err != nil {
		return nil, err
	}
	return newPending(Snapshot{
		ID:                             id,
		Origin:                         OriginExternal,
		Kind:                           cmd.Kind,
		WalletID:                       cmd.WalletID,
		PlayerID:                       cmd.PlayerID,
		Money:                          cmd.Money,
		ProviderID:                     cmd.ProviderID,
		ExternalTransactionID:          cmd.ExternalTransactionID,
		IdempotencyKey:                 cmd.IdempotencyKey,
		PayloadHash:                    cmd.PayloadHash(),
		RoundID:                        cmd.RoundID,
		GameID:                         cmd.GameID,
		ReferenceExternalTransactionID: cmd.ReferenceExternalTransactionID,
		CorrelationID:                  correlationID,
	}, now)
}

func validateIdentifiers(cmd Command) error {
	for _, f := range [...]struct{ name, value string }{
		{"provider id", cmd.ProviderID},
		{"external transaction id", cmd.ExternalTransactionID},
		{"round id", cmd.RoundID},
		{"game id", cmd.GameID},
		{"reference", cmd.ReferenceExternalTransactionID},
	} {
		if f.value != "" && !ValidTextID(f.value) {
			return invalidTransaction("the " + f.name + " is not in the format of a text id")
		}
	}
	if cmd.IdempotencyKey != "" && !isIdempotencyKey(cmd.IdempotencyKey) {
		return invalidTransaction("the idempotency key is not in its format")
	}
	if cmd.ReferenceExternalTransactionID != "" && cmd.ReferenceExternalTransactionID == cmd.ExternalTransactionID {
		return invalidTransaction("a transaction cannot refer to itself")
	}
	return nil
}

// NewOpening starts the PENDING transaction that credits the initial balance of a wallet.
func NewOpening(id, walletID, playerID uuid.UUID, amount money.Money, correlationID string, now time.Time) (*Transaction, error) {
	return newPending(Snapshot{
		ID:            id,
		Origin:        OriginInternal,
		Kind:          KindOpening,
		WalletID:      walletID,
		PlayerID:      playerID,
		Money:         amount,
		CorrelationID: correlationID,
	}, now)
}

func newPending(s Snapshot, now time.Time) (*Transaction, error) {
	s.Status = StatusPending
	s.CreatedAt = now
	s.UpdatedAt = now
	return Rehydrate(s)
}

// Result is the outcome of applying a transaction to its wallet.
type Result struct {
	BalanceAfter           money.Money
	WalletVersion          int64
	ReferenceTransactionID uuid.NullUUID
}

// MarkProcessed ends the transaction as PROCESSED, recording the resulting wallet state.
func (t *Transaction) MarkProcessed(r Result, now time.Time) error {
	s, err := t.begin(now, StatusPending, StatusPendingReference)
	if err != nil {
		return err
	}
	s.Status = StatusProcessed
	s.ResultBalance = &r.BalanceAfter
	s.ResultWalletVersion = r.WalletVersion
	s.ReferenceTransactionID = r.ReferenceTransactionID
	s.ProcessedAt = &now
	s.NextAttemptAt, s.ExpiresAt = nil, nil
	return t.commit(s, now)
}

// Reject ends the transaction as REJECTED, recording the failure code and observed wallet state.
func (t *Transaction) Reject(code FailureCode, observed money.Money, walletVersion int64, now time.Time) error {
	s, err := t.begin(now, StatusPending, StatusPendingReference)
	if err != nil {
		return err
	}
	s.Status = StatusRejected
	s.FailureCode = code
	s.ResultBalance = nil
	if observed.IsValid() {
		s.ResultBalance = &observed
	}
	s.ResultWalletVersion = walletVersion
	s.ProcessedAt = &now
	s.NextAttemptAt, s.ExpiresAt = nil, nil
	return t.commit(s, now)
}

// AwaitReference moves a PENDING transaction to PENDING_REFERENCE.
func (t *Transaction) AwaitReference(next, expires, now time.Time) error {
	s, err := t.begin(now, StatusPending)
	if err != nil {
		return err
	}
	if !next.After(now) || !expires.After(now) || expires.Before(next) {
		return invalidTransaction("the next attempt and the expiry must be after now, and the expiry not before the attempt")
	}
	s.Status = StatusPendingReference
	s.NextAttemptAt, s.ExpiresAt = &next, &expires
	return t.commit(s, now)
}

// RescheduleReference counts a missed look for the reference and schedules the next one.
func (t *Transaction) RescheduleReference(next, now time.Time) error {
	s, err := t.begin(now, StatusPendingReference)
	if err != nil {
		return err
	}
	if !next.After(now) {
		return invalidTransaction("the next attempt must be after now")
	}
	s.ReferenceAttempts++
	s.NextAttemptAt = &next
	return t.commit(s, now)
}

// NotePermanentFailure counts a permanent error and ends the transaction as FAILED at limit.
func (t *Transaction) NotePermanentFailure(limit int, now time.Time) error {
	s, err := t.begin(now, StatusPending, StatusPendingReference)
	if err != nil {
		return err
	}
	if limit < 1 {
		return invalidTransaction("the limit of permanent failures must be at least 1")
	}
	s.PermanentFailures++
	if s.PermanentFailures >= limit {
		s.Status = StatusFailed
		s.FailureCode = FailureProcessingFailed
		s.ProcessedAt = &now
		s.NextAttemptAt, s.ExpiresAt = nil, nil
	}
	return t.commit(s, now)
}

func (t *Transaction) begin(now time.Time, from ...Status) (Snapshot, error) {
	if !slices.Contains(from, t.state.Status) {
		if t.state.Status.IsTerminal() {
			return Snapshot{}, ErrTerminalState
		}
		return Snapshot{}, ErrInvalidTransition
	}
	if now.IsZero() {
		return Snapshot{}, invalidTransaction("the time of the transition is zero")
	}
	return t.state.clone(), nil
}

func (t *Transaction) commit(next Snapshot, now time.Time) error {
	if now.After(next.UpdatedAt) {
		next.UpdatedAt = now
	}
	if err := next.validate(); err != nil {
		return err
	}
	t.state = next
	return nil
}

// ID returns the identifier of the transaction.
func (t *Transaction) ID() uuid.UUID { return t.state.ID }

// Origin returns who started the transaction.
func (t *Transaction) Origin() Origin { return t.state.Origin }

// Kind returns the kind of the transaction.
func (t *Transaction) Kind() Kind { return t.state.Kind }

// Status returns the position of the transaction in its lifecycle.
func (t *Transaction) Status() Status { return t.state.Status }

// WalletID returns the identifier of the wallet the transaction acts on.
func (t *Transaction) WalletID() uuid.UUID { return t.state.WalletID }

// PlayerID returns the identifier of the player the transaction is for.
func (t *Transaction) PlayerID() uuid.UUID { return t.state.PlayerID }

// Money returns the amount of the transaction.
func (t *Transaction) Money() money.Money { return t.state.Money }

// ProviderID returns the provider that sent the transaction, or "" for an OPENING.
func (t *Transaction) ProviderID() string { return t.state.ProviderID }

// ExternalTransactionID returns the provider's id for the transaction, or "" for an OPENING.
func (t *Transaction) ExternalTransactionID() string { return t.state.ExternalTransactionID }

// IdempotencyKey returns the key the transaction was sent under, or "" for an OPENING.
func (t *Transaction) IdempotencyKey() string { return t.state.IdempotencyKey }

// PayloadHash returns the hash of the command, or "" for an OPENING.
func (t *Transaction) PayloadHash() string { return t.state.PayloadHash }

// RoundID returns the round of the game, or "" for an OPENING.
func (t *Transaction) RoundID() string { return t.state.RoundID }

// GameID returns the game, or "" for an OPENING.
func (t *Transaction) GameID() string { return t.state.GameID }

// ReferenceExternalTransactionID returns the provider id of the referenced transaction, or "".
func (t *Transaction) ReferenceExternalTransactionID() string {
	return t.state.ReferenceExternalTransactionID
}

// ReferenceTransactionID returns the transaction the reference resolved to, if any.
func (t *Transaction) ReferenceTransactionID() uuid.NullUUID { return t.state.ReferenceTransactionID }

// FailureCode returns why the transaction ended as REJECTED or FAILED, or "".
func (t *Transaction) FailureCode() FailureCode { return t.state.FailureCode }

// ResultBalance returns a copy of the wallet balance recorded at decision, or nil.
func (t *Transaction) ResultBalance() *money.Money { return clonePtr(t.state.ResultBalance) }

// ResultWalletVersion returns the wallet version recorded at decision, or 0.
func (t *Transaction) ResultWalletVersion() int64 { return t.state.ResultWalletVersion }

// CorrelationID returns the correlation id of the request that created the transaction.
func (t *Transaction) CorrelationID() string { return t.state.CorrelationID }

// ReferenceAttempts returns how many looks for the reference missed.
func (t *Transaction) ReferenceAttempts() int { return t.state.ReferenceAttempts }

// PermanentFailures returns how many permanent errors the transaction met.
func (t *Transaction) PermanentFailures() int { return t.state.PermanentFailures }

// NextAttemptAt returns when the transaction is to be looked at again, or nil.
func (t *Transaction) NextAttemptAt() *time.Time { return clonePtr(t.state.NextAttemptAt) }

// ExpiresAt returns when the wait for the reference ends, or nil.
func (t *Transaction) ExpiresAt() *time.Time { return clonePtr(t.state.ExpiresAt) }

// CreatedAt returns when the transaction was created.
func (t *Transaction) CreatedAt() time.Time { return t.state.CreatedAt }

// UpdatedAt returns when the transaction last changed.
func (t *Transaction) UpdatedAt() time.Time { return t.state.UpdatedAt }

// ProcessedAt returns when the transaction reached a final status, or nil.
func (t *Transaction) ProcessedAt() *time.Time { return clonePtr(t.state.ProcessedAt) }
