package wallet

import (
	"errors"
	"fmt"
)

// Sentinel errors of the package, matched with errors.Is.
var (
	ErrInvalidWallet      = errors.New("wallet: invalid wallet")
	ErrInsufficientFunds  = errors.New("wallet: insufficient funds")
	ErrCurrencyMismatch   = errors.New("wallet: currency mismatch")
	ErrNonPositiveAmount  = errors.New("wallet: amount must be positive")
	ErrBalanceLimit       = errors.New("wallet: balance limit exceeded")
	ErrInvalidLedgerEntry = errors.New("wallet: invalid ledger entry")
)

func invalidWallet(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidWallet, reason)
}

func invalidEntry(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidLedgerEntry, reason)
}
