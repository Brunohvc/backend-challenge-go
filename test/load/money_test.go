//go:build load

package main

import "testing"

func TestMinorRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		amount string
		minor  int64
	}{
		{"0.00", 0},
		{"0.01", 1},
		{"25.00", 2500},
		{"10000000.00", 1_000_000_000},
		{"-1.50", -150},
	}
	for _, tc := range tests {
		got, err := parseMinor(tc.amount)
		if err != nil || got != tc.minor {
			t.Errorf("parseMinor(%q) = %d, %v, want %d", tc.amount, got, err, tc.minor)
		}
		if s := formatMinor(tc.minor); s != tc.amount {
			t.Errorf("formatMinor(%d) = %q, want %q", tc.minor, s, tc.amount)
		}
	}
}

func TestParseMinor_Invalid(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"", "1", "1.0", "1.000", ".50", "1,00", "a.bc", "1e2", "--1.00", "99999999999999999.99"} {
		if got, err := parseMinor(in); err == nil {
			t.Errorf("parseMinor(%q) = %d, want an error", in, got)
		}
	}
}

func TestBRL(t *testing.T) {
	t.Parallel()

	if got := brl(2550); got != (money{Amount: "25.50", Currency: "BRL"}) {
		t.Errorf("brl(2550) = %+v", got)
	}
}
