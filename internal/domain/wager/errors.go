package wager

import (
	"errors"
	"fmt"
)

// Sentinel errors of the package, matched with errors.Is.
var (
	ErrInvalidTransition  = errors.New("wager: invalid transition")
	ErrTerminalState      = errors.New("wager: transaction is in a final state")
	ErrInvalidTransaction = errors.New("wager: invalid transaction")
	ErrInvalidDecision    = errors.New("wager: invalid decision input")
)

func invalidTransaction(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidTransaction, reason)
}

func invalidDecision(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidDecision, reason)
}
