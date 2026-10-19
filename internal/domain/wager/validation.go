package wager

import (
	"errors"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
)

// ValidationCode names the problem that made a command invalid.
type ValidationCode string

// The validation codes, from the most specific to the most generic.
const (
	ValidationInvalidIdempotencyKey ValidationCode = "INVALID_IDEMPOTENCY_KEY"
	ValidationInvalidKind           ValidationCode = "INVALID_KIND"
	ValidationInvalidMoney          ValidationCode = "INVALID_MONEY"
	ValidationUnsupportedCurrency   ValidationCode = "UNSUPPORTED_CURRENCY"
	ValidationInvalidAmountForKind  ValidationCode = "INVALID_AMOUNT_FOR_KIND"
	ValidationReferenceRequired     ValidationCode = "REFERENCE_REQUIRED"
	ValidationInvalidRequest        ValidationCode = "INVALID_REQUEST"
)

var validationPrecedence = [...]ValidationCode{
	ValidationInvalidIdempotencyKey,
	ValidationInvalidKind,
	ValidationInvalidMoney,
	ValidationUnsupportedCurrency,
	ValidationInvalidAmountForKind,
	ValidationReferenceRequired,
	ValidationInvalidRequest,
}

const (
	pathProviderID            = "providerId"
	pathExternalTransactionID = "externalTransactionId"
	pathIdempotencyKey        = "idempotencyKey"
	pathPlayerID              = "playerId"
	pathWalletID              = "walletId"
	pathRoundID               = "roundId"
	pathGameID                = "gameId"
	pathKind                  = "kind"
	pathMoneyAmount           = "money.amount"
	pathMoneyCurrency         = "money.currency"
	pathReference             = "referenceExternalTransactionId"
)

const (
	fieldCodeRequired       = "REQUIRED"
	fieldCodeInvalidFormat  = "INVALID_FORMAT"
	fieldCodeNotAllowed     = "NOT_ALLOWED"
	fieldCodeUnsupported    = "UNSUPPORTED"
	fieldCodeMustBeZero     = "MUST_BE_ZERO"
	fieldCodeMustBePositive = "MUST_BE_POSITIVE"
	fieldCodeSelfReference  = "SELF_REFERENCE"
)

// FieldError is one problem found in one field of a command.
type FieldError struct {
	Field string
	Code  string
}

// ValidationError is the error NewCommand returns for an invalid input.
type ValidationError struct {
	Code   ValidationCode
	Fields []FieldError
}

// Error renders the code and the invalid fields on one line, without their values.
func (e *ValidationError) Error() string {
	msg := "wager: invalid command: " + string(e.Code)
	if len(e.Fields) == 0 {
		return msg
	}
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Code
	}
	return msg + " (" + strings.Join(parts, ", ") + ")"
}

type validator struct {
	fields []FieldError
	code   ValidationCode
}

func (v *validator) add(code ValidationCode, path, fieldCode string) {
	v.fields = append(v.fields, FieldError{Field: path, Code: fieldCode})
	if v.code == "" || precedence(code) < precedence(v.code) {
		v.code = code
	}
}

func (v *validator) err() error {
	if len(v.fields) == 0 {
		return nil
	}
	return &ValidationError{Code: v.code, Fields: v.fields}
}

func precedence(code ValidationCode) int {
	return slices.Index(validationPrecedence[:], code)
}

func (v *validator) textID(path, s string) {
	if !ValidTextID(s) {
		v.add(ValidationInvalidRequest, path, requiredOr(s, fieldCodeInvalidFormat))
	}
}

func (v *validator) idempotencyKey(s string) {
	if !isIdempotencyKey(s) {
		v.add(ValidationInvalidIdempotencyKey, pathIdempotencyKey, requiredOr(s, fieldCodeInvalidFormat))
	}
}

func (v *validator) uuid(path, s string) uuid.UUID {
	id, err := uuid.Parse(s)
	if err != nil || id == uuid.Nil || id.String() != s {
		v.add(ValidationInvalidRequest, path, requiredOr(s, fieldCodeInvalidFormat))
		return uuid.Nil
	}
	return id
}

func (v *validator) kind(s string) Kind {
	k, ok := parseExternalKind(s)
	if !ok {
		v.add(ValidationInvalidKind, pathKind, requiredOr(s, fieldCodeUnsupported))
	}
	return k
}

func (v *validator) moneyFor(kind Kind, amount, currency string) money.Money {
	c, currencyErr := money.ParseCurrency(currency)
	amountCheckCurrency := c
	if currencyErr != nil {
		amountCheckCurrency = money.BRL()
	}

	m, err := money.Parse(amount, amountCheckCurrency.Code())
	var parseErr *money.ParseError
	switch {
	case err == nil:
		v.amountForKind(kind, m)
	case errors.As(err, &parseErr):
		v.add(ValidationInvalidMoney, pathMoneyAmount, string(parseErr.Reason))
	default:
		v.add(ValidationInvalidMoney, pathMoneyAmount, string(money.ReasonMalformed))
	}

	if currencyErr != nil {
		v.add(ValidationUnsupportedCurrency, pathMoneyCurrency, requiredOr(currency, fieldCodeUnsupported))
		return money.Money{}
	}
	return m
}

func (v *validator) amountForKind(kind Kind, amount money.Money) {
	switch {
	case kind.mustBeZero() && !amount.IsZero():
		v.add(ValidationInvalidAmountForKind, pathMoneyAmount, fieldCodeMustBeZero)
	case kind.mustBePositive() && !amount.IsPositive():
		v.add(ValidationInvalidAmountForKind, pathMoneyAmount, fieldCodeMustBePositive)
	}
}

func (v *validator) reference(kind Kind, ext, ref string) {
	switch {
	case ref != "" && kind.forbidsReference():
		v.add(ValidationInvalidRequest, pathReference, fieldCodeNotAllowed)
	case ref != "" && !ValidTextID(ref):
		v.add(ValidationInvalidRequest, pathReference, fieldCodeInvalidFormat)
	case ref != "" && ref == ext:
		v.add(ValidationInvalidRequest, pathReference, fieldCodeSelfReference)
	case ref == "" && kind.requiresReference():
		v.add(ValidationReferenceRequired, pathReference, fieldCodeRequired)
	}
}

func requiredOr(s, rule string) string {
	if s == "" {
		return fieldCodeRequired
	}
	return rule
}

const (
	maxTextIDLength         = 128
	maxIdempotencyKeyLength = 255
)

// ValidTextID reports whether s matches ^[A-Za-z0-9._:-]{1,128}$.
func ValidTextID(s string) bool {
	if len(s) == 0 || len(s) > maxTextIDLength {
		return false
	}
	for i := range len(s) {
		c := s[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case c == '.', c == '_', c == ':', c == '-':
		default:
			return false
		}
	}
	return true
}

func isIdempotencyKey(s string) bool {
	if len(s) == 0 || len(s) > maxIdempotencyKeyLength {
		return false
	}
	for i := range len(s) {
		if s[i] < 0x21 || s[i] > 0x7E {
			return false
		}
	}
	return true
}
