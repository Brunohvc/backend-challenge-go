package money

import "errors"

// Sentinel errors of the package, matched with errors.Is.
var (
	ErrInvalidAmount       = errors.New("money: invalid amount")
	ErrUnsupportedCurrency = errors.New("money: unsupported currency")
	ErrOverflow            = errors.New("money: overflow")
	ErrCurrencyMismatch    = errors.New("money: currency mismatch")
	ErrUninitialized       = errors.New("money: uninitialized value")
)

// ParseReason names the amount format rule that Parse rejected.
type ParseReason string

// The reasons Parse rejects an amount.
const (
	ReasonEmpty              ParseReason = "EMPTY"
	ReasonNotANumber         ParseReason = "NOT_A_NUMBER"
	ReasonScientificNotation ParseReason = "SCIENTIFIC_NOTATION"
	ReasonWhitespace         ParseReason = "WHITESPACE"
	ReasonSigned             ParseReason = "SIGNED"
	ReasonMalformed          ParseReason = "MALFORMED"
	ReasonMissingScale       ParseReason = "MISSING_SCALE"
	ReasonScaleTooSmall      ParseReason = "SCALE_TOO_SMALL"
	ReasonScaleExceeded      ParseReason = "SCALE_EXCEEDED"
	ReasonLeadingZero        ParseReason = "LEADING_ZERO"
	ReasonOverflow           ParseReason = "OVERFLOW"
)

// ParseError is the error Parse returns for a rejected amount.
type ParseError struct {
	Reason ParseReason
}

// Error implements error.
func (e *ParseError) Error() string {
	return "money: invalid amount: " + string(e.Reason)
}

// Unwrap returns ErrOverflow or ErrInvalidAmount, depending on the reason.
func (e *ParseError) Unwrap() error {
	if e.Reason == ReasonOverflow {
		return ErrOverflow
	}
	return ErrInvalidAmount
}
