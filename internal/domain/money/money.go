// Package money is the value object for amounts of money.
package money

import (
	"encoding/json"
	"math"
	"strconv"
)

const (
	fractionDigits = 2
	minorPerUnit   = 100
)

// Money is an amount in a single currency; the zero value is invalid.
type Money struct {
	minor    int64
	currency Currency
}

// FromMinor builds a Money from minor units, negative amounts included.
func FromMinor(minor int64, c Currency) (Money, error) {
	if c.IsZero() {
		return Money{}, ErrUninitialized
	}
	return Money{minor: minor, currency: c}, nil
}

// Zero returns an amount of zero in c.
func Zero(c Currency) (Money, error) {
	return FromMinor(0, c)
}

// Minor returns the amount in minor units: 2500 for 25.00.
func (m Money) Minor() int64 { return m.minor }

// Currency returns the currency of m, or the zero Currency when m is invalid.
func (m Money) Currency() Currency { return m.currency }

// IsValid reports whether m carries a currency.
func (m Money) IsValid() bool { return !m.currency.IsZero() }

// IsZero reports whether m is a valid amount of exactly zero.
func (m Money) IsZero() bool { return m.IsValid() && m.minor == 0 }

// IsPositive reports whether m is a valid amount greater than zero.
func (m Money) IsPositive() bool { return m.IsValid() && m.minor > 0 }

// IsNegative reports whether m is a valid amount less than zero.
func (m Money) IsNegative() bool { return m.IsValid() && m.minor < 0 }

// Add returns m + o, failing on invalid operands, mixed currencies or overflow.
func (m Money) Add(o Money) (Money, error) {
	if err := m.requireSameCurrency(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) ||
		(o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

// Sub returns m - o, which may be negative.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.requireSameCurrency(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor < math.MinInt64+o.minor) ||
		(o.minor < 0 && m.minor > math.MaxInt64+o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor - o.minor, currency: m.currency}, nil
}

// Equal reports whether m and o are valid and hold the same amount and currency.
func (m Money) Equal(o Money) bool {
	return m.IsValid() && m == o
}

// String returns the canonical amount without the currency, such as "25.00".
func (m Money) String() string {
	if !m.IsValid() {
		return ""
	}
	return formatMinor(m.minor)
}

type wireMoney struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// MarshalJSON encodes m as {"amount":"25.00","currency":"BRL"}.
func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsValid() {
		return nil, ErrUninitialized
	}
	return json.Marshal(wireMoney{Amount: m.String(), Currency: m.currency.Code()})
}

func (m Money) requireSameCurrency(o Money) error {
	if !m.IsValid() || !o.IsValid() {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return ErrCurrencyMismatch
	}
	return nil
}

func formatMinor(minor int64) string {
	nonPositive := minor
	if nonPositive > 0 {
		nonPositive = -nonPositive
	}
	units, cents := -(nonPositive / minorPerUnit), -(nonPositive % minorPerUnit)

	buf := make([]byte, 0, 24)
	if minor < 0 {
		buf = append(buf, '-')
	}
	buf = strconv.AppendInt(buf, units, 10)
	buf = append(buf, '.', byte('0'+cents/10), byte('0'+cents%10))
	return string(buf)
}
