//go:build load

package main

import (
	"fmt"
	"strconv"
	"strings"
)

const currency = "BRL"

type money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func brl(minor int64) money {
	return money{Amount: formatMinor(minor), Currency: currency}
}

func formatMinor(minor int64) string {
	sign := ""
	if minor < 0 {
		sign, minor = "-", -minor
	}
	return fmt.Sprintf("%s%d.%02d", sign, minor/100, minor%100)
}

func parseMinor(amount string) (int64, error) {
	digits := strings.TrimPrefix(amount, "-")
	units, cents, ok := strings.Cut(digits, ".")
	if !ok || units == "" || len(cents) != 2 || !allDigits(units) || !allDigits(cents) {
		return 0, fmt.Errorf("amount %q is not a decimal with two places", amount)
	}
	v, err := strconv.ParseInt(units+cents, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("amount %q: %w", amount, err)
	}
	if digits != amount {
		v = -v
	}
	return v, nil
}

func allDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
