package money

const (
	codeBRL = "BRL"
	codeUSD = "USD"
	codeEUR = "EUR"
)

// Currency is a supported currency; the zero value means no currency.
type Currency struct {
	code string
}

// BRL returns the Brazilian real.
func BRL() Currency { return Currency{code: codeBRL} }

// ParseCurrency returns the Currency for an exact, uppercase ISO 4217 code.
func ParseCurrency(code string) (Currency, error) {
	switch code {
	case codeBRL, codeUSD, codeEUR:
		return Currency{code: code}, nil
	}
	return Currency{}, ErrUnsupportedCurrency
}

// Code returns the ISO 4217 code, or "" for the zero Currency.
func (c Currency) Code() string { return c.code }

// IsZero reports whether c is the zero Currency, meaning "no currency".
func (c Currency) IsZero() bool { return c.code == "" }
