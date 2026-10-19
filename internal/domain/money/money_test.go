package money_test

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
)

func minor(t *testing.T, units int64, c money.Currency) money.Money {
	t.Helper()
	m, err := money.FromMinor(units, c)
	if err != nil {
		t.Fatalf("FromMinor(%d, %q) error = %v", units, c.Code(), err)
	}
	return m
}

func currency(t *testing.T, code string) money.Currency {
	t.Helper()
	c, err := money.ParseCurrency(code)
	if err != nil {
		t.Fatalf("ParseCurrency(%q) error = %v", code, err)
	}
	return c
}

func brl(t *testing.T, units int64) money.Money { return minor(t, units, money.BRL()) }

func usd(t *testing.T, units int64) money.Money { return minor(t, units, currency(t, "USD")) }

func TestParseCurrency(t *testing.T) {
	tests := []struct {
		code string
		want string
		err  error
	}{
		{"BRL", "BRL", nil},
		{"USD", "USD", nil},
		{"EUR", "EUR", nil},
		{"brl", "", money.ErrUnsupportedCurrency},
		{"JPY", "", money.ErrUnsupportedCurrency},
		{"", "", money.ErrUnsupportedCurrency},
		{"BRLX", "", money.ErrUnsupportedCurrency},
		{" BRL", "", money.ErrUnsupportedCurrency},
	}
	for _, tc := range tests {
		t.Run(tc.code, func(t *testing.T) {
			got, err := money.ParseCurrency(tc.code)
			if !errors.Is(err, tc.err) {
				t.Fatalf("ParseCurrency(%q) error = %v, want %v", tc.code, err, tc.err)
			}
			if got.Code() != tc.want {
				t.Errorf("ParseCurrency(%q) = %q, want %q", tc.code, got.Code(), tc.want)
			}
		})
	}
	if money.BRL() != currency(t, "BRL") {
		t.Errorf("BRL() = %q, want the currency ParseCurrency returns for BRL", money.BRL().Code())
	}
}

func TestCurrency_CodeAndIsZero(t *testing.T) {
	for _, code := range []string{"BRL", "USD", "EUR"} {
		if c := currency(t, code); c.IsZero() {
			t.Errorf("%q.IsZero() = true, want false", c.Code())
		}
	}
	if !(money.Currency{}).IsZero() {
		t.Error("Currency{}.IsZero() = false, want true")
	}
	if got := (money.Currency{}).Code(); got != "" {
		t.Errorf("Currency{}.Code() = %q, want empty", got)
	}
}

func TestFromMinor(t *testing.T) {
	tests := []struct {
		name  string
		minor int64
	}{
		{"zero", 0},
		{"positive", 2500},
		{"negative", -2500},
		{"max", math.MaxInt64},
		{"min", math.MinInt64},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := money.FromMinor(tc.minor, currency(t, "USD"))
			if err != nil {
				t.Fatalf("FromMinor(%d, USD) error = %v", tc.minor, err)
			}
			if got.Minor() != tc.minor || got.Currency() != currency(t, "USD") || !got.IsValid() {
				t.Errorf("FromMinor(%d, USD) = %d %q (valid=%v)", tc.minor, got.Minor(), got.Currency().Code(), got.IsValid())
			}
		})
	}
}

func TestZero(t *testing.T) {
	eur := currency(t, "EUR")
	got, err := money.Zero(eur)
	if err != nil {
		t.Fatalf("Zero(EUR) error = %v", err)
	}
	if !got.IsZero() || got.Currency() != eur {
		t.Errorf("Zero(EUR) = %s %q, want 0.00 EUR", got, got.Currency().Code())
	}

	if _, err := money.Zero(money.Currency{}); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Zero(Currency{}) error = %v, want ErrUninitialized", err)
	}
}

func TestMoney_Predicates(t *testing.T) {
	tests := []struct {
		name                        string
		m                           money.Money
		valid, zero, positive, negt bool
	}{
		{"zero", brl(t, 0), true, true, false, false},
		{"positive", brl(t, 1), true, false, true, false},
		{"negative", brl(t, -1), true, false, false, true},
		{"max", brl(t, math.MaxInt64), true, false, true, false},
		{"min", brl(t, math.MinInt64), true, false, false, true},
		{"uninitialized", money.Money{}, false, false, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.m.IsValid(); got != tc.valid {
				t.Errorf("IsValid() = %v, want %v", got, tc.valid)
			}
			if got := tc.m.IsZero(); got != tc.zero {
				t.Errorf("IsZero() = %v, want %v", got, tc.zero)
			}
			if got := tc.m.IsPositive(); got != tc.positive {
				t.Errorf("IsPositive() = %v, want %v", got, tc.positive)
			}
			if got := tc.m.IsNegative(); got != tc.negt {
				t.Errorf("IsNegative() = %v, want %v", got, tc.negt)
			}
		})
	}
}

func TestMoney_Arithmetic(t *testing.T) {
	tests := []struct {
		name    string
		a, b    int64
		wantAdd int64
		wantSub int64
	}{
		{"both positive", 1000, 550, 1550, 450},
		{"sub goes negative", 500, 700, 1200, -200},
		{"zero operands", 0, 0, 0, 0},
		{"negative plus positive", -300, 1000, 700, -1300},
		{"both negative", -300, -200, -500, -100},
		{"cancels out", 2500, 2500, 5000, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, b := brl(t, tc.a), brl(t, tc.b)

			sum, err := a.Add(b)
			if err != nil {
				t.Fatalf("Add error = %v", err)
			}
			if sum.Minor() != tc.wantAdd || sum.Currency() != money.BRL() {
				t.Errorf("%d + %d = %d %q, want %d BRL", tc.a, tc.b, sum.Minor(), sum.Currency().Code(), tc.wantAdd)
			}

			diff, err := a.Sub(b)
			if err != nil {
				t.Fatalf("Sub error = %v", err)
			}
			if diff.Minor() != tc.wantSub || diff.Currency() != money.BRL() {
				t.Errorf("%d - %d = %d %q, want %d BRL", tc.a, tc.b, diff.Minor(), diff.Currency().Code(), tc.wantSub)
			}
		})
	}
}

func TestMoney_SubCanGoNegative(t *testing.T) {
	five, err := money.Parse("5.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	seven, err := money.Parse("7.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	got, err := five.Sub(seven)
	if err != nil {
		t.Fatalf("5.00 - 7.00 error = %v", err)
	}
	if got.String() != "-2.00" || !got.IsNegative() {
		t.Errorf("5.00 - 7.00 = %s, want -2.00", got)
	}
}

func TestMoney_Overflow(t *testing.T) {
	tests := []struct {
		name string
		op   func() (money.Money, error)
	}{
		{"max plus one", func() (money.Money, error) { return brl(t, math.MaxInt64).Add(brl(t, 1)) }},
		{"one plus max", func() (money.Money, error) { return brl(t, 1).Add(brl(t, math.MaxInt64)) }},
		{"max plus max", func() (money.Money, error) { return brl(t, math.MaxInt64).Add(brl(t, math.MaxInt64)) }},
		{"min plus minus one", func() (money.Money, error) { return brl(t, math.MinInt64).Add(brl(t, -1)) }},
		{"min plus min", func() (money.Money, error) { return brl(t, math.MinInt64).Add(brl(t, math.MinInt64)) }},
		{"min minus one", func() (money.Money, error) { return brl(t, math.MinInt64).Sub(brl(t, 1)) }},
		{"max minus minus one", func() (money.Money, error) { return brl(t, math.MaxInt64).Sub(brl(t, -1)) }},
		{"zero minus min", func() (money.Money, error) { return brl(t, 0).Sub(brl(t, math.MinInt64)) }},
		{"max minus min", func() (money.Money, error) { return brl(t, math.MaxInt64).Sub(brl(t, math.MinInt64)) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.op()
			if !errors.Is(err, money.ErrOverflow) {
				t.Fatalf("error = %v, want ErrOverflow", err)
			}
			if got != (money.Money{}) {
				t.Errorf("returned %v alongside ErrOverflow, want the zero Money", got)
			}
		})
	}
}

func TestMoney_BoundaryResultsFit(t *testing.T) {
	tests := []struct {
		name string
		op   func() (money.Money, error)
		want int64
	}{
		{"max plus zero", func() (money.Money, error) { return brl(t, math.MaxInt64).Add(brl(t, 0)) }, math.MaxInt64},
		{"max plus minus one", func() (money.Money, error) { return brl(t, math.MaxInt64).Add(brl(t, -1)) }, math.MaxInt64 - 1},
		{"max minus one plus one", func() (money.Money, error) { return brl(t, math.MaxInt64-1).Add(brl(t, 1)) }, math.MaxInt64},
		{"min plus one", func() (money.Money, error) { return brl(t, math.MinInt64).Add(brl(t, 1)) }, math.MinInt64 + 1},
		{"minus one plus min", func() (money.Money, error) { return brl(t, -1).Add(brl(t, math.MinInt64+1)) }, math.MinInt64},
		{"min minus zero", func() (money.Money, error) { return brl(t, math.MinInt64).Sub(brl(t, 0)) }, math.MinInt64},
		{"min plus one minus one", func() (money.Money, error) { return brl(t, math.MinInt64+1).Sub(brl(t, 1)) }, math.MinInt64},
		{"max minus max", func() (money.Money, error) { return brl(t, math.MaxInt64).Sub(brl(t, math.MaxInt64)) }, 0},
		{"min minus min", func() (money.Money, error) { return brl(t, math.MinInt64).Sub(brl(t, math.MinInt64)) }, 0},
		{"minus one minus min", func() (money.Money, error) { return brl(t, -1).Sub(brl(t, math.MinInt64)) }, math.MaxInt64},
		{"zero minus max", func() (money.Money, error) { return brl(t, 0).Sub(brl(t, math.MaxInt64)) }, -math.MaxInt64},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.op()
			if err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
			if got.Minor() != tc.want {
				t.Errorf("result = %d, want %d", got.Minor(), tc.want)
			}
		})
	}
}

func TestMoney_CurrencyMismatch(t *testing.T) {
	a, b := brl(t, 1000), usd(t, 1000)

	if _, err := a.Add(b); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("Add error = %v, want ErrCurrencyMismatch", err)
	}
	if _, err := a.Sub(b); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("Sub error = %v, want ErrCurrencyMismatch", err)
	}
	if a.Equal(b) {
		t.Error("Equal across currencies = true, want false")
	}
}

func TestMoney_ZeroValueRejected(t *testing.T) {
	var zero money.Money
	valid := brl(t, 1000)

	if _, err := zero.Add(valid); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Money{}.Add error = %v, want ErrUninitialized", err)
	}
	if _, err := valid.Add(zero); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Add(Money{}) error = %v, want ErrUninitialized", err)
	}
	if _, err := zero.Sub(valid); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Money{}.Sub error = %v, want ErrUninitialized", err)
	}
	if _, err := valid.Sub(zero); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Sub(Money{}) error = %v, want ErrUninitialized", err)
	}
	if _, err := zero.MarshalJSON(); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Money{}.MarshalJSON error = %v, want ErrUninitialized", err)
	}
	if _, err := money.FromMinor(1, money.Currency{}); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("FromMinor(1, Currency{}) error = %v, want ErrUninitialized", err)
	}

	if zero.IsValid() {
		t.Error("Money{}.IsValid() = true, want false")
	}
	if zero.Equal(money.Money{}) {
		t.Error("Money{}.Equal(Money{}) = true, want false")
	}
	if zero.Equal(valid) || valid.Equal(zero) {
		t.Error("Equal with an uninitialized Money = true, want false")
	}
	if zero.String() != "" {
		t.Errorf("Money{}.String() = %q, want empty", zero.String())
	}
}

func TestMoney_Equal(t *testing.T) {
	tests := []struct {
		name string
		a, b money.Money
		want bool
	}{
		{"same amount and currency", brl(t, 2500), brl(t, 2500), true},
		{"different amount", brl(t, 2500), brl(t, 2501), false},
		{"different currency", brl(t, 2500), usd(t, 2500), false},
		{"zero of the same currency", brl(t, 0), brl(t, 0), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Equal(tc.b); got != tc.want {
				t.Errorf("Equal = %v, want %v", got, tc.want)
			}
			if got := tc.b.Equal(tc.a); got != tc.want {
				t.Errorf("Equal (swapped) = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMoney_StringAndJSON(t *testing.T) {
	tests := []struct {
		name     string
		m        money.Money
		wantText string
		wantJSON string
	}{
		{"whole amount", brl(t, 2500), "25.00", `{"amount":"25.00","currency":"BRL"}`},
		{"zero", brl(t, 0), "0.00", `{"amount":"0.00","currency":"BRL"}`},
		{"cents only", usd(t, 5), "0.05", `{"amount":"0.05","currency":"USD"}`},
		{"one cent", usd(t, 1), "0.01", `{"amount":"0.01","currency":"USD"}`},
		{"tens of cents", minor(t, 10, currency(t, "EUR")), "0.10", `{"amount":"0.10","currency":"EUR"}`},
		{"negative", brl(t, -205), "-2.05", `{"amount":"-2.05","currency":"BRL"}`},
		{"negative cents only", brl(t, -5), "-0.05", `{"amount":"-0.05","currency":"BRL"}`},
		{"thousands", brl(t, 100000), "1000.00", `{"amount":"1000.00","currency":"BRL"}`},
		{"max", brl(t, math.MaxInt64), "92233720368547758.07", `{"amount":"92233720368547758.07","currency":"BRL"}`},
		{"min", brl(t, math.MinInt64), "-92233720368547758.08", `{"amount":"-92233720368547758.08","currency":"BRL"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.m.String(); got != tc.wantText {
				t.Errorf("String() = %q, want %q", got, tc.wantText)
			}

			got, err := tc.m.MarshalJSON()
			if err != nil {
				t.Fatalf("MarshalJSON error = %v", err)
			}
			if string(got) != tc.wantJSON {
				t.Errorf("MarshalJSON = %s, want %s", got, tc.wantJSON)
			}
		})
	}
}

func TestMoney_JSONInsideValues(t *testing.T) {
	type balance struct {
		Available money.Money  `json:"available"`
		Held      *money.Money `json:"held"`
	}
	held := usd(t, -150)

	tests := []struct {
		name string
		in   any
		want string
	}{
		{"by value", brl(t, 2500), `{"amount":"25.00","currency":"BRL"}`},
		{"by pointer", &held, `{"amount":"-1.50","currency":"USD"}`},
		{"struct field", balance{Available: brl(t, 2500), Held: &held},
			`{"available":{"amount":"25.00","currency":"BRL"},"held":{"amount":"-1.50","currency":"USD"}}`},
		{"slice", []money.Money{brl(t, 1), brl(t, 2)},
			`[{"amount":"0.01","currency":"BRL"},{"amount":"0.02","currency":"BRL"}]`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatalf("json.Marshal error = %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("json.Marshal = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestMoney_JSONRejectsUninitializedField(t *testing.T) {
	type balance struct {
		Available money.Money `json:"available"`
	}

	_, err := json.Marshal(balance{})

	if !errors.Is(err, money.ErrUninitialized) {
		t.Fatalf("json.Marshal(zero field) error = %v, want ErrUninitialized", err)
	}
}

func TestMoney_Immutable(t *testing.T) {
	a, b := brl(t, 1000), brl(t, 250)

	sum, err := a.Add(b)
	if err != nil {
		t.Fatalf("Add error = %v", err)
	}
	diff, err := a.Sub(b)
	if err != nil {
		t.Fatalf("Sub error = %v", err)
	}

	if a.Minor() != 1000 || b.Minor() != 250 {
		t.Errorf("operands changed to %d and %d, want 1000 and 250", a.Minor(), b.Minor())
	}
	if sum.Minor() != 1250 || diff.Minor() != 750 {
		t.Errorf("results = %d, %d, want 1250, 750", sum.Minor(), diff.Minor())
	}
}

func TestMoney_StringParseRoundTrip(t *testing.T) {
	for _, units := range []int64{0, 1, 5, 99, 100, 101, 2500, 100000, 123456789, math.MaxInt64 - 1, math.MaxInt64} {
		m := brl(t, units)

		got, err := money.Parse(m.String(), m.Currency().Code())
		if err != nil {
			t.Fatalf("Parse(%q) error = %v", m.String(), err)
		}
		if !got.Equal(m) {
			t.Errorf("Parse(%q) = %d minor units, want %d", m.String(), got.Minor(), units)
		}
	}
}
