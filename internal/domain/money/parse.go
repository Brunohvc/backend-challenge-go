package money

import (
	"fmt"
	"math"
	"strings"
	"unicode"
)

// Parse reads an external amount such as "25.00" and pairs it with a currency.
func Parse(amount, currency string) (Money, error) {
	minor, amountErr := parseMinor(amount)
	c, currencyErr := ParseCurrency(currency)
	switch {
	case amountErr != nil && currencyErr != nil:
		return Money{}, fmt.Errorf("%w; %w", amountErr, currencyErr)
	case amountErr != nil:
		return Money{}, amountErr
	case currencyErr != nil:
		return Money{}, currencyErr
	}
	return Money{minor: minor, currency: c}, nil
}

func parseMinor(amount string) (int64, error) {
	if amount == "" {
		return 0, reject(ReasonEmpty)
	}
	if isNonFinite(amount) {
		return 0, reject(ReasonNotANumber)
	}
	if strings.ContainsAny(amount, "eE") {
		return 0, reject(ReasonScientificNotation)
	}
	if strings.ContainsFunc(amount, unicode.IsSpace) {
		return 0, reject(ReasonWhitespace)
	}
	if amount[0] == '+' || amount[0] == '-' {
		return 0, reject(ReasonSigned)
	}

	whole, fraction, hasPoint := strings.Cut(amount, ".")
	if whole == "" || !onlyDigits(whole) || !onlyDigits(fraction) {
		return 0, reject(ReasonMalformed)
	}
	if !hasPoint {
		return 0, reject(ReasonMissingScale)
	}
	if len(fraction) > fractionDigits {
		return 0, reject(ReasonScaleExceeded)
	}
	if len(fraction) < fractionDigits {
		return 0, reject(ReasonScaleTooSmall)
	}
	if len(whole) > 1 && whole[0] == '0' {
		return 0, reject(ReasonLeadingZero)
	}

	var minor int64
	for _, digits := range [...]string{whole, fraction} {
		for _, r := range digits {
			digit := int64(r - '0')
			if appendingOverflows(minor, digit) {
				return 0, reject(ReasonOverflow)
			}
			minor = minor*10 + digit
		}
	}
	return minor, nil
}

func appendingOverflows(minor, digit int64) bool {
	return minor > (math.MaxInt64-digit)/10
}

func reject(reason ParseReason) error {
	return &ParseError{Reason: reason}
}

func isNonFinite(s string) bool {
	if s != "" && (s[0] == '+' || s[0] == '-') {
		s = s[1:]
	}
	return strings.EqualFold(s, "nan") || strings.EqualFold(s, "inf") || strings.EqualFold(s, "infinity")
}

func onlyDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
