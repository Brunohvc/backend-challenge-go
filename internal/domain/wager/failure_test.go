package wager_test

import (
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

func TestFailureCode_Values(t *testing.T) {
	tests := []struct {
		code wager.FailureCode
		want string
	}{
		{wager.FailureInsufficientFunds, "INSUFFICIENT_FUNDS"},
		{wager.FailureReversalInsufficientFunds, "REVERSAL_INSUFFICIENT_FUNDS"},
		{wager.FailureCurrencyMismatch, "CURRENCY_MISMATCH"},
		{wager.FailureWalletPlayerMismatch, "WALLET_PLAYER_MISMATCH"},
		{wager.FailureReferenceNotFound, "REFERENCE_NOT_FOUND"},
		{wager.FailureReferenceNotProcessed, "REFERENCE_NOT_PROCESSED"},
		{wager.FailureReferenceMismatch, "REFERENCE_MISMATCH"},
		{wager.FailureInvalidReferenceKind, "INVALID_REFERENCE_KIND"},
		{wager.FailureReversalAmountMismatch, "REVERSAL_AMOUNT_MISMATCH"},
		{wager.FailureReferenceAlreadyReversed, "REFERENCE_ALREADY_REVERSED"},
		{wager.FailureBalanceLimitExceeded, "BALANCE_LIMIT_EXCEEDED"},
		{wager.FailureProcessingFailed, "PROCESSING_FAILED"},
	}
	for _, tc := range tests {
		if string(tc.code) != tc.want {
			t.Errorf("failure code = %q, want %q", tc.code, tc.want)
		}
	}
}
