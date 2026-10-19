package wagering

import "errors"

// The errors of the service, matched with errors.Is.
var (
	ErrIdempotencyKeyReused         = errors.New("wagering: idempotency key reused for another command")
	ErrDuplicateExternalTransaction = errors.New("wagering: external transaction already submitted under another idempotency key")
	ErrWalletNotFound               = errors.New("wagering: wallet not found")
	ErrTransactionNotFound          = errors.New("wagering: transaction not found")
	ErrMessageIDReused              = errors.New("wagering: message id reused for another payload")
)
