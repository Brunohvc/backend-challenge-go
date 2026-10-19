package money_test

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
)

func TestParse_Valid(t *testing.T) {
	tests := []struct {
		amount string
		want   int64
	}{
		{"0.00", 0},
		{"0.01", 1},
		{"0.10", 10},
		{"9.99", 999},
		{"25.00", 2500},
		{"100.99", 10099},
		{"1000.00", 100000},
		{"92233720368547758.07", math.MaxInt64},
	}
	for _, tc := range tests {
		t.Run(tc.amount, func(t *testing.T) {
			got, err := money.Parse(tc.amount, "BRL")
			if err != nil {
				t.Fatalf("Parse(%q, BRL) error = %v", tc.amount, err)
			}
			if got.Minor() != tc.want {
				t.Errorf("Parse(%q, BRL).Minor() = %d, want %d", tc.amount, got.Minor(), tc.want)
			}
			if got.Currency() != money.BRL() {
				t.Errorf("Parse(%q, BRL).Currency() = %q, want BRL", tc.amount, got.Currency().Code())
			}
		})
	}
}

func TestParse_KeepsRequestedCurrency(t *testing.T) {
	for _, code := range []string{"BRL", "USD", "EUR"} {
		t.Run(code, func(t *testing.T) {
			got, err := money.Parse("1.00", code)
			if err != nil {
				t.Fatalf("Parse(1.00, %s) error = %v", code, err)
			}
			if got.Currency().Code() != code {
				t.Errorf("currency = %q, want %q", got.Currency().Code(), code)
			}
		})
	}
}

func TestParse_Invalid(t *testing.T) {
	tests := []struct {
		reason money.ParseReason
		inputs []string
	}{
		{money.ReasonEmpty, []string{""}},
		{money.ReasonNotANumber, []string{"NaN", "nan", "inf", "Inf", "-Infinity", "+inf", "INFINITY"}},
		{money.ReasonScientificNotation, []string{"1e3", "2.5E2", "1.00e2", "e"}},
		{money.ReasonWhitespace, []string{" 1.00", "1.00 ", "1 .00", "\t1.00", "1.00\n", "1.00 "}},
		{money.ReasonSigned, []string{"-1.00", "+1.00", "-0.00", "-", "+", "++1.00"}},
		{money.ReasonMalformed, []string{"1,00", "abc", ".50", "1..0", "1.0.0", ".", "1.0a", "$1.00", "٣.00", "1.٠٠"}},
		{money.ReasonMissingScale, []string{"25", "0", "1000"}},
		{money.ReasonScaleTooSmall, []string{"25.5", "25.", "0.0", "1.9"}},
		{money.ReasonScaleExceeded, []string{"25.001", "0.000", "1.2345"}},
		{money.ReasonLeadingZero, []string{"007.00", "00.00", "01.00"}},
		{money.ReasonOverflow, []string{
			"92233720368547758.08",
			"92233720368547759.00",
			"922337203685477580.00",
			"99999999999999999999999999.99",
		}},
	}
	for _, tc := range tests {
		t.Run(string(tc.reason), func(t *testing.T) {
			for _, input := range tc.inputs {
				t.Run(strconv.Quote(input), func(t *testing.T) {
					got, err := money.Parse(input, "BRL")
					if err == nil {
						t.Fatalf("Parse(%q, BRL) = %s, want error", input, got)
					}
					if got != (money.Money{}) {
						t.Errorf("Parse(%q, BRL) returned %v alongside an error, want the zero Money", input, got)
					}

					var parseErr *money.ParseError
					if !errors.As(err, &parseErr) {
						t.Fatalf("Parse(%q, BRL) error = %T, want *money.ParseError", input, err)
					}
					if parseErr.Reason != tc.reason {
						t.Errorf("Parse(%q, BRL) reason = %s, want %s", input, parseErr.Reason, tc.reason)
					}

					wantSentinel, otherSentinel := money.ErrInvalidAmount, money.ErrOverflow
					if tc.reason == money.ReasonOverflow {
						wantSentinel, otherSentinel = money.ErrOverflow, money.ErrInvalidAmount
					}
					if !errors.Is(err, wantSentinel) {
						t.Errorf("errors.Is(%v, %v) = false, want true", err, wantSentinel)
					}
					if errors.Is(err, otherSentinel) {
						t.Errorf("errors.Is(%v, %v) = true, want false", err, otherSentinel)
					}
				})
			}
		})
	}
}

func TestParse_ReportsAmountAndCurrency(t *testing.T) {
	got, err := money.Parse("25", "JPY")

	var parseErr *money.ParseError
	if !errors.As(err, &parseErr) || parseErr.Reason != money.ReasonMissingScale {
		t.Errorf("Parse(25, JPY) error = %v, want a MISSING_SCALE ParseError", err)
	}
	if !errors.Is(err, money.ErrUnsupportedCurrency) {
		t.Errorf("Parse(25, JPY) error = %v, want it to match ErrUnsupportedCurrency too", err)
	}
	if strings.Contains(err.Error(), "\n") {
		t.Errorf("Parse(25, JPY) error = %q, want one line for the log", err)
	}
	if got != (money.Money{}) {
		t.Errorf("Parse(25, JPY) returned %v alongside an error, want the zero Money", got)
	}
}

func TestParse_ReportsOnlyWhatIsWrong(t *testing.T) {
	_, err := money.Parse("25", "BRL")
	if errors.Is(err, money.ErrUnsupportedCurrency) {
		t.Errorf("Parse(25, BRL) error = %v, want no currency problem", err)
	}
	_, err = money.Parse("25.00", "JPY")
	var parseErr *money.ParseError
	if errors.As(err, &parseErr) {
		t.Errorf("Parse(25.00, JPY) error = %v, want no amount problem", err)
	}
}

func TestParse_UnsupportedCurrency(t *testing.T) {
	for _, code := range []string{"brl", "JPY", "", "BRLX", " BRL"} {
		t.Run(strconv.Quote(code), func(t *testing.T) {
			got, err := money.Parse("25.00", code)
			if !errors.Is(err, money.ErrUnsupportedCurrency) {
				t.Fatalf("Parse(25.00, %q) error = %v, want ErrUnsupportedCurrency", code, err)
			}
			if got != (money.Money{}) {
				t.Errorf("Parse(25.00, %q) returned %v alongside an error, want the zero Money", code, got)
			}
		})
	}
}

func TestParseError_Message(t *testing.T) {
	const input = "do-not-log-this-input"

	_, err := money.Parse(input, "BRL")

	if err == nil {
		t.Fatal("Parse accepted a malformed amount")
	}
	if !strings.Contains(err.Error(), string(money.ReasonMalformed)) {
		t.Errorf("error %q does not name the reason %s", err, money.ReasonMalformed)
	}
	if strings.Contains(err.Error(), input) {
		t.Errorf("error %q echoes the rejected input", err)
	}
}

func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		"", "0.00", "25.00", "92233720368547758.07", "92233720368547758.08",
		"-1.00", "+1.00", "1e3", "NaN", " 1.00", "007.00", "25", "25.5", "25.001", "1,00", ".50",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		got, err := money.Parse(input, "BRL")
		if err != nil {
			var parseErr *money.ParseError
			if !errors.As(err, &parseErr) {
				t.Fatalf("Parse(%q) error = %T, want *money.ParseError", input, err)
			}
			if !errors.Is(err, money.ErrInvalidAmount) && !errors.Is(err, money.ErrOverflow) {
				t.Fatalf("Parse(%q) error %v matches neither ErrInvalidAmount nor ErrOverflow", input, err)
			}
			return
		}
		if got.Minor() < 0 {
			t.Fatalf("Parse(%q) = %d minor units, want a non-negative amount", input, got.Minor())
		}
		if got.String() != input {
			t.Fatalf("Parse(%q).String() = %q, want the input back", input, got.String())
		}
	})
}
