package wager

// FailureCode says why a transaction ended as REJECTED or FAILED.
type FailureCode string

// The failure codes, each a final outcome.
const (
	FailureInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	FailureCurrencyMismatch          FailureCode = "CURRENCY_MISMATCH"
	FailureWalletPlayerMismatch      FailureCode = "WALLET_PLAYER_MISMATCH"
	FailureReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferenceNotProcessed     FailureCode = "REFERENCE_NOT_PROCESSED"
	FailureReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	FailureInvalidReferenceKind      FailureCode = "INVALID_REFERENCE_KIND"
	FailureReversalAmountMismatch    FailureCode = "REVERSAL_AMOUNT_MISMATCH"
	FailureReferenceAlreadyReversed  FailureCode = "REFERENCE_ALREADY_REVERSED"
	FailureBalanceLimitExceeded      FailureCode = "BALANCE_LIMIT_EXCEEDED"
	FailureProcessingFailed          FailureCode = "PROCESSING_FAILED"
)

func (c FailureCode) isRejection() bool {
	switch c {
	case FailureInsufficientFunds,
		FailureReversalInsufficientFunds,
		FailureCurrencyMismatch,
		FailureWalletPlayerMismatch,
		FailureReferenceNotFound,
		FailureReferenceNotProcessed,
		FailureReferenceMismatch,
		FailureInvalidReferenceKind,
		FailureReversalAmountMismatch,
		FailureReferenceAlreadyReversed,
		FailureBalanceLimitExceeded:
		return true
	}
	return false
}
