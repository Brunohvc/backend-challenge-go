package wager_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
)

const (
	playerUUID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
	walletUUID = "0192f291-27dd-7d3f-8071-5f8685deef37"
	nilUUID    = "00000000-0000-0000-0000-000000000000"
)

func betInput() wager.CommandInput {
	return wager.CommandInput{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		IdempotencyKey:        "0192f293-5b3e-7a41-9c52-1f6d0b8e3a77",
		PlayerID:              playerUUID,
		WalletID:              walletUUID,
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  "BET",
		Amount:                "25.00",
		Currency:              "BRL",
	}
}

func edited(edit func(*wager.CommandInput)) wager.CommandInput {
	in := betInput()
	edit(&in)
	return in
}

func fe(field, code string) wager.FieldError {
	return wager.FieldError{Field: field, Code: code}
}

func parsed(t *testing.T, amount, currency string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, currency)
	if err != nil {
		t.Fatalf("money.Parse(%q, %q) error = %v", amount, currency, err)
	}
	return m
}

func wantCommand(t *testing.T, in wager.CommandInput) wager.Command {
	t.Helper()
	return wager.Command{
		ProviderID:                     in.ProviderID,
		ExternalTransactionID:          in.ExternalTransactionID,
		IdempotencyKey:                 in.IdempotencyKey,
		PlayerID:                       uuid.MustParse(in.PlayerID),
		WalletID:                       uuid.MustParse(in.WalletID),
		RoundID:                        in.RoundID,
		GameID:                         in.GameID,
		Kind:                           wager.Kind(in.Kind),
		Money:                          parsed(t, in.Amount, in.Currency),
		ReferenceExternalTransactionID: in.ReferenceExternalTransactionID,
	}
}

func TestNewCommand_Valid(t *testing.T) {
	const allKeySymbols = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"

	tests := []struct {
		name string
		edit func(*wager.CommandInput)
	}{
		{"BET", func(*wager.CommandInput) {}},
		{"WIN without reference", func(in *wager.CommandInput) {
			in.Kind, in.Amount = "WIN", "50.00"
		}},
		{"WIN with reference", func(in *wager.CommandInput) {
			in.Kind, in.Amount, in.ReferenceExternalTransactionID = "WIN", "50.00", "transaction-122"
		}},
		{"LOSS with zero", func(in *wager.CommandInput) {
			in.Kind, in.Amount = "LOSS", "0.00"
		}},
		{"REFUND", func(in *wager.CommandInput) {
			in.Kind, in.ReferenceExternalTransactionID = "REFUND", "transaction-122"
		}},
		{"ROLLBACK", func(in *wager.CommandInput) {
			in.Kind, in.ReferenceExternalTransactionID = "ROLLBACK", "transaction-122"
		}},
		{"USD", func(in *wager.CommandInput) { in.Currency = "USD" }},
		{"EUR", func(in *wager.CommandInput) { in.Currency = "EUR" }},
		{"smallest positive amount", func(in *wager.CommandInput) { in.Amount = "0.01" }},
		{"largest amount", func(in *wager.CommandInput) { in.Amount = "92233720368547758.07" }},
		{"text ids at the length limit", func(in *wager.CommandInput) {
			limit := strings.Repeat("a", 128)
			in.ProviderID, in.ExternalTransactionID, in.RoundID, in.GameID = limit, limit, limit, limit
			in.Kind, in.ReferenceExternalTransactionID = "WIN", strings.Repeat("b", 128)
		}},
		{"reference that differs from the transaction only in case", func(in *wager.CommandInput) {
			in.Kind, in.ReferenceExternalTransactionID = "REFUND", "Transaction-123"
		}},
		{"every character of the text id alphabet", func(in *wager.CommandInput) {
			in.ProviderID = "Az09._:-"
		}},
		{"idempotency key at the length limit", func(in *wager.CommandInput) {
			in.IdempotencyKey = strings.Repeat("k", 255)
		}},
		{"idempotency key with every printable symbol", func(in *wager.CommandInput) {
			in.IdempotencyKey = allKeySymbols
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := edited(tc.edit)
			got, err := wager.NewCommand(in)
			if err != nil {
				t.Fatalf("NewCommand() error = %v", err)
			}
			if want := wantCommand(t, in); got != want {
				t.Errorf("NewCommand() = %+v, want %+v", got, want)
			}
		})
	}
}

func TestNewCommand_Invalid(t *testing.T) {
	tests := []struct {
		name string
		edit func(*wager.CommandInput)
		code wager.ValidationCode
		want []wager.FieldError
	}{
		{"empty providerId", func(in *wager.CommandInput) { in.ProviderID = "" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("providerId", "REQUIRED")}},
		{"providerId with leading space", func(in *wager.CommandInput) { in.ProviderID = " provider-a" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("providerId", "INVALID_FORMAT")}},
		{"providerId with trailing space", func(in *wager.CommandInput) { in.ProviderID = "provider-a " },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("providerId", "INVALID_FORMAT")}},
		{"providerId with trailing newline", func(in *wager.CommandInput) { in.ProviderID = "provider-a\n" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("providerId", "INVALID_FORMAT")}},
		{"providerId of 129 characters", func(in *wager.CommandInput) { in.ProviderID = strings.Repeat("a", 129) },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("providerId", "INVALID_FORMAT")}},
		{"providerId with slash", func(in *wager.CommandInput) { in.ProviderID = "provider/a" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("providerId", "INVALID_FORMAT")}},
		{"empty externalTransactionId", func(in *wager.CommandInput) { in.ExternalTransactionID = "" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("externalTransactionId", "REQUIRED")}},
		{"externalTransactionId with space", func(in *wager.CommandInput) { in.ExternalTransactionID = "transaction 123" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("externalTransactionId", "INVALID_FORMAT")}},
		{"roundId with space", func(in *wager.CommandInput) { in.RoundID = "round 987" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("roundId", "INVALID_FORMAT")}},
		{"empty roundId", func(in *wager.CommandInput) { in.RoundID = "" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("roundId", "REQUIRED")}},
		{"gameId with non-ASCII letters", func(in *wager.CommandInput) { in.GameID = "jogo-ção" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("gameId", "INVALID_FORMAT")}},
		{"empty gameId", func(in *wager.CommandInput) { in.GameID = "" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("gameId", "REQUIRED")}},

		{"empty idempotency key", func(in *wager.CommandInput) { in.IdempotencyKey = "" },
			wager.ValidationInvalidIdempotencyKey, []wager.FieldError{fe("idempotencyKey", "REQUIRED")}},
		{"idempotency key with space", func(in *wager.CommandInput) { in.IdempotencyKey = "key 1" },
			wager.ValidationInvalidIdempotencyKey, []wager.FieldError{fe("idempotencyKey", "INVALID_FORMAT")}},
		{"idempotency key with tab", func(in *wager.CommandInput) { in.IdempotencyKey = "key\t1" },
			wager.ValidationInvalidIdempotencyKey, []wager.FieldError{fe("idempotencyKey", "INVALID_FORMAT")}},
		{"idempotency key with DEL", func(in *wager.CommandInput) { in.IdempotencyKey = "key\x7f1" },
			wager.ValidationInvalidIdempotencyKey, []wager.FieldError{fe("idempotencyKey", "INVALID_FORMAT")}},
		{"idempotency key with non-ASCII letters", func(in *wager.CommandInput) { in.IdempotencyKey = "chave-ção" },
			wager.ValidationInvalidIdempotencyKey, []wager.FieldError{fe("idempotencyKey", "INVALID_FORMAT")}},
		{"idempotency key of 256 characters", func(in *wager.CommandInput) { in.IdempotencyKey = strings.Repeat("k", 256) },
			wager.ValidationInvalidIdempotencyKey, []wager.FieldError{fe("idempotencyKey", "INVALID_FORMAT")}},

		{"empty playerId", func(in *wager.CommandInput) { in.PlayerID = "" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("playerId", "REQUIRED")}},
		{"playerId not a UUID", func(in *wager.CommandInput) { in.PlayerID = "player-1" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("playerId", "INVALID_FORMAT")}},
		{"uppercase playerId", func(in *wager.CommandInput) { in.PlayerID = strings.ToUpper(playerUUID) },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("playerId", "INVALID_FORMAT")}},
		{"playerId with braces", func(in *wager.CommandInput) { in.PlayerID = "{" + playerUUID + "}" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("playerId", "INVALID_FORMAT")}},
		{"playerId with urn prefix", func(in *wager.CommandInput) { in.PlayerID = "urn:uuid:" + playerUUID },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("playerId", "INVALID_FORMAT")}},
		{"playerId without hyphens", func(in *wager.CommandInput) { in.PlayerID = strings.ReplaceAll(playerUUID, "-", "") },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("playerId", "INVALID_FORMAT")}},
		{"playerId with surrounding space", func(in *wager.CommandInput) { in.PlayerID = " " + playerUUID },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("playerId", "INVALID_FORMAT")}},
		{"nil playerId", func(in *wager.CommandInput) { in.PlayerID = nilUUID },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("playerId", "INVALID_FORMAT")}},
		{"empty walletId", func(in *wager.CommandInput) { in.WalletID = "" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("walletId", "REQUIRED")}},
		{"uppercase walletId", func(in *wager.CommandInput) { in.WalletID = strings.ToUpper(walletUUID) },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("walletId", "INVALID_FORMAT")}},
		{"walletId with braces", func(in *wager.CommandInput) { in.WalletID = "{" + walletUUID + "}" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("walletId", "INVALID_FORMAT")}},
		{"nil walletId", func(in *wager.CommandInput) { in.WalletID = nilUUID },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("walletId", "INVALID_FORMAT")}},

		{"OPENING from outside", func(in *wager.CommandInput) { in.Kind = "OPENING" },
			wager.ValidationInvalidKind, []wager.FieldError{fe("kind", "UNSUPPORTED")}},
		{"unknown kind", func(in *wager.CommandInput) { in.Kind = "DEPOSIT" },
			wager.ValidationInvalidKind, []wager.FieldError{fe("kind", "UNSUPPORTED")}},
		{"lowercase kind", func(in *wager.CommandInput) { in.Kind = "bet" },
			wager.ValidationInvalidKind, []wager.FieldError{fe("kind", "UNSUPPORTED")}},
		{"kind with surrounding space", func(in *wager.CommandInput) { in.Kind = " BET" },
			wager.ValidationInvalidKind, []wager.FieldError{fe("kind", "UNSUPPORTED")}},
		{"empty kind", func(in *wager.CommandInput) { in.Kind = "" },
			wager.ValidationInvalidKind, []wager.FieldError{fe("kind", "REQUIRED")}},

		{"empty amount", func(in *wager.CommandInput) { in.Amount = "" },
			wager.ValidationInvalidMoney, []wager.FieldError{fe("money.amount", "EMPTY")}},
		{"NaN amount", func(in *wager.CommandInput) { in.Amount = "NaN" },
			wager.ValidationInvalidMoney, []wager.FieldError{fe("money.amount", "NOT_A_NUMBER")}},
		{"scientific amount", func(in *wager.CommandInput) { in.Amount = "1e3" },
			wager.ValidationInvalidMoney, []wager.FieldError{fe("money.amount", "SCIENTIFIC_NOTATION")}},
		{"amount with space", func(in *wager.CommandInput) { in.Amount = " 25.00" },
			wager.ValidationInvalidMoney, []wager.FieldError{fe("money.amount", "WHITESPACE")}},
		{"negative amount", func(in *wager.CommandInput) { in.Amount = "-25.00" },
			wager.ValidationInvalidMoney, []wager.FieldError{fe("money.amount", "SIGNED")}},
		{"amount that is not a number", func(in *wager.CommandInput) { in.Amount = "abc" },
			wager.ValidationInvalidMoney, []wager.FieldError{fe("money.amount", "MALFORMED")}},
		{"amount without scale", func(in *wager.CommandInput) { in.Amount = "25" },
			wager.ValidationInvalidMoney, []wager.FieldError{fe("money.amount", "MISSING_SCALE")}},
		{"amount with one decimal", func(in *wager.CommandInput) { in.Amount = "25.5" },
			wager.ValidationInvalidMoney, []wager.FieldError{fe("money.amount", "SCALE_TOO_SMALL")}},
		{"amount with three decimals", func(in *wager.CommandInput) { in.Amount = "25.001" },
			wager.ValidationInvalidMoney, []wager.FieldError{fe("money.amount", "SCALE_EXCEEDED")}},
		{"amount with leading zero", func(in *wager.CommandInput) { in.Amount = "025.00" },
			wager.ValidationInvalidMoney, []wager.FieldError{fe("money.amount", "LEADING_ZERO")}},
		{"amount past int64", func(in *wager.CommandInput) { in.Amount = "92233720368547758.08" },
			wager.ValidationInvalidMoney, []wager.FieldError{fe("money.amount", "OVERFLOW")}},

		{"lowercase currency", func(in *wager.CommandInput) { in.Currency = "brl" },
			wager.ValidationUnsupportedCurrency, []wager.FieldError{fe("money.currency", "UNSUPPORTED")}},
		{"currency outside the list", func(in *wager.CommandInput) { in.Currency = "GBP" },
			wager.ValidationUnsupportedCurrency, []wager.FieldError{fe("money.currency", "UNSUPPORTED")}},
		{"currency with space", func(in *wager.CommandInput) { in.Currency = " BRL" },
			wager.ValidationUnsupportedCurrency, []wager.FieldError{fe("money.currency", "UNSUPPORTED")}},
		{"empty currency", func(in *wager.CommandInput) { in.Currency = "" },
			wager.ValidationUnsupportedCurrency, []wager.FieldError{fe("money.currency", "REQUIRED")}},

		{"BET of zero", func(in *wager.CommandInput) { in.Amount = "0.00" },
			wager.ValidationInvalidAmountForKind, []wager.FieldError{fe("money.amount", "MUST_BE_POSITIVE")}},
		{"WIN of zero", func(in *wager.CommandInput) { in.Kind, in.Amount = "WIN", "0.00" },
			wager.ValidationInvalidAmountForKind, []wager.FieldError{fe("money.amount", "MUST_BE_POSITIVE")}},
		{"REFUND of zero", func(in *wager.CommandInput) {
			in.Kind, in.Amount, in.ReferenceExternalTransactionID = "REFUND", "0.00", "transaction-122"
		}, wager.ValidationInvalidAmountForKind, []wager.FieldError{fe("money.amount", "MUST_BE_POSITIVE")}},
		{"ROLLBACK of zero", func(in *wager.CommandInput) {
			in.Kind, in.Amount, in.ReferenceExternalTransactionID = "ROLLBACK", "0.00", "transaction-122"
		}, wager.ValidationInvalidAmountForKind, []wager.FieldError{fe("money.amount", "MUST_BE_POSITIVE")}},
		{"LOSS of more than zero", func(in *wager.CommandInput) { in.Kind, in.Amount = "LOSS", "1.00" },
			wager.ValidationInvalidAmountForKind, []wager.FieldError{fe("money.amount", "MUST_BE_ZERO")}},
		{"zero policy is not checked for an unknown kind", func(in *wager.CommandInput) {
			in.Kind, in.Amount = "OPENING", "0.00"
		}, wager.ValidationInvalidKind, []wager.FieldError{fe("kind", "UNSUPPORTED")}},
		{"zero policy is not checked for a malformed amount", func(in *wager.CommandInput) {
			in.Kind, in.Amount = "LOSS", "0"
		}, wager.ValidationInvalidMoney, []wager.FieldError{fe("money.amount", "MISSING_SCALE")}},

		{"REFUND without reference", func(in *wager.CommandInput) { in.Kind = "REFUND" },
			wager.ValidationReferenceRequired, []wager.FieldError{fe("referenceExternalTransactionId", "REQUIRED")}},
		{"ROLLBACK without reference", func(in *wager.CommandInput) { in.Kind = "ROLLBACK" },
			wager.ValidationReferenceRequired, []wager.FieldError{fe("referenceExternalTransactionId", "REQUIRED")}},
		{"BET with reference", func(in *wager.CommandInput) { in.ReferenceExternalTransactionID = "transaction-122" },
			wager.ValidationInvalidRequest, []wager.FieldError{fe("referenceExternalTransactionId", "NOT_ALLOWED")}},
		{"LOSS with reference", func(in *wager.CommandInput) {
			in.Kind, in.Amount, in.ReferenceExternalTransactionID = "LOSS", "0.00", "transaction-122"
		}, wager.ValidationInvalidRequest, []wager.FieldError{fe("referenceExternalTransactionId", "NOT_ALLOWED")}},
		{"BET with a malformed reference is still not allowed", func(in *wager.CommandInput) {
			in.ReferenceExternalTransactionID = "transaction 122"
		}, wager.ValidationInvalidRequest, []wager.FieldError{fe("referenceExternalTransactionId", "NOT_ALLOWED")}},
		{"WIN with malformed reference", func(in *wager.CommandInput) {
			in.Kind, in.ReferenceExternalTransactionID = "WIN", " transaction-122"
		}, wager.ValidationInvalidRequest, []wager.FieldError{fe("referenceExternalTransactionId", "INVALID_FORMAT")}},
		{"REFUND with malformed reference", func(in *wager.CommandInput) {
			in.Kind, in.ReferenceExternalTransactionID = "REFUND", strings.Repeat("r", 129)
		}, wager.ValidationInvalidRequest, []wager.FieldError{fe("referenceExternalTransactionId", "INVALID_FORMAT")}},
		{"malformed reference with an unknown kind", func(in *wager.CommandInput) {
			in.Kind, in.ReferenceExternalTransactionID = "OPENING", "transaction 122"
		}, wager.ValidationInvalidKind, []wager.FieldError{
			fe("kind", "UNSUPPORTED"), fe("referenceExternalTransactionId", "INVALID_FORMAT"),
		}},
		{"missing reference is not reported for an unknown kind", func(in *wager.CommandInput) {
			in.Kind = "SETTLE"
		}, wager.ValidationInvalidKind, []wager.FieldError{fe("kind", "UNSUPPORTED")}},

		{"REFUND referring to itself", func(in *wager.CommandInput) {
			in.Kind, in.ReferenceExternalTransactionID = "REFUND", in.ExternalTransactionID
		}, wager.ValidationInvalidRequest, []wager.FieldError{fe("referenceExternalTransactionId", "SELF_REFERENCE")}},
		{"ROLLBACK referring to itself", func(in *wager.CommandInput) {
			in.Kind, in.ReferenceExternalTransactionID = "ROLLBACK", in.ExternalTransactionID
		}, wager.ValidationInvalidRequest, []wager.FieldError{fe("referenceExternalTransactionId", "SELF_REFERENCE")}},
		{"WIN referring to itself", func(in *wager.CommandInput) {
			in.Kind, in.ReferenceExternalTransactionID = "WIN", in.ExternalTransactionID
		}, wager.ValidationInvalidRequest, []wager.FieldError{fe("referenceExternalTransactionId", "SELF_REFERENCE")}},
		{"BET referring to itself is still not allowed", func(in *wager.CommandInput) {
			in.ReferenceExternalTransactionID = in.ExternalTransactionID
		}, wager.ValidationInvalidRequest, []wager.FieldError{fe("referenceExternalTransactionId", "NOT_ALLOWED")}},
		{"self reference with a malformed id reports the format", func(in *wager.CommandInput) {
			in.Kind, in.ExternalTransactionID, in.ReferenceExternalTransactionID = "REFUND", "transaction 123", "transaction 123"
		}, wager.ValidationInvalidRequest, []wager.FieldError{
			fe("externalTransactionId", "INVALID_FORMAT"), fe("referenceExternalTransactionId", "INVALID_FORMAT"),
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := wager.NewCommand(edited(tc.edit))
			assertInvalid(t, got, err, tc.code, tc.want)
		})
	}
}

func assertInvalid(t *testing.T, got wager.Command, err error, code wager.ValidationCode, fields []wager.FieldError) {
	t.Helper()
	var verr *wager.ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("NewCommand() error = %v (%T), want *ValidationError", err, err)
	}
	if verr.Code != code {
		t.Errorf("Code = %q, want %q", verr.Code, code)
	}
	if !slices.Equal(verr.Fields, fields) {
		t.Errorf("Fields = %+v, want %+v", verr.Fields, fields)
	}
	if got != (wager.Command{}) {
		t.Errorf("NewCommand() returned %+v with an error, want the zero Command", got)
	}
}

func TestNewCommand_ReportsEveryProblem(t *testing.T) {
	tests := []struct {
		name string
		in   wager.CommandInput
		code wager.ValidationCode
		want []wager.FieldError
	}{
		{
			name: "idempotency key beats everything",
			in: edited(func(in *wager.CommandInput) {
				in.ProviderID, in.IdempotencyKey, in.Kind, in.Amount, in.Currency = "", "bad key", "OPENING", "1", "XXX"
			}),
			code: wager.ValidationInvalidIdempotencyKey,
			want: []wager.FieldError{
				fe("providerId", "REQUIRED"),
				fe("idempotencyKey", "INVALID_FORMAT"),
				fe("kind", "UNSUPPORTED"),
				fe("money.amount", "MISSING_SCALE"),
				fe("money.currency", "UNSUPPORTED"),
			},
		},
		{
			name: "kind beats money",
			in: edited(func(in *wager.CommandInput) {
				in.RoundID, in.Kind, in.Amount, in.Currency = "round 1", "OPENING", "-1.00", "XXX"
			}),
			code: wager.ValidationInvalidKind,
			want: []wager.FieldError{
				fe("roundId", "INVALID_FORMAT"),
				fe("kind", "UNSUPPORTED"),
				fe("money.amount", "SIGNED"),
				fe("money.currency", "UNSUPPORTED"),
			},
		},
		{
			name: "money beats currency",
			in: edited(func(in *wager.CommandInput) {
				in.GameID, in.Amount, in.Currency = "jogo-ção", "25", "brl"
			}),
			code: wager.ValidationInvalidMoney,
			want: []wager.FieldError{
				fe("gameId", "INVALID_FORMAT"),
				fe("money.amount", "MISSING_SCALE"),
				fe("money.currency", "UNSUPPORTED"),
			},
		},
		{
			name: "currency beats amount for kind",
			in: edited(func(in *wager.CommandInput) {
				in.Amount, in.Currency = "0.00", "XXX"
			}),
			code: wager.ValidationUnsupportedCurrency,
			want: []wager.FieldError{
				fe("money.amount", "MUST_BE_POSITIVE"),
				fe("money.currency", "UNSUPPORTED"),
			},
		},
		{
			name: "amount for kind beats reference required",
			in: edited(func(in *wager.CommandInput) {
				in.Kind, in.Amount = "REFUND", "0.00"
			}),
			code: wager.ValidationInvalidAmountForKind,
			want: []wager.FieldError{
				fe("money.amount", "MUST_BE_POSITIVE"),
				fe("referenceExternalTransactionId", "REQUIRED"),
			},
		},
		{
			name: "reference required beats invalid request",
			in: edited(func(in *wager.CommandInput) {
				in.ExternalTransactionID, in.PlayerID, in.Kind = "tx 1", "player", "ROLLBACK"
			}),
			code: wager.ValidationReferenceRequired,
			want: []wager.FieldError{
				fe("externalTransactionId", "INVALID_FORMAT"),
				fe("playerId", "INVALID_FORMAT"),
				fe("referenceExternalTransactionId", "REQUIRED"),
			},
		},
		{
			name: "invalid request alone",
			in: edited(func(in *wager.CommandInput) {
				in.WalletID, in.RoundID = strings.ToUpper(walletUUID), ""
			}),
			code: wager.ValidationInvalidRequest,
			want: []wager.FieldError{
				fe("walletId", "INVALID_FORMAT"),
				fe("roundId", "REQUIRED"),
			},
		},
		{
			name: "an empty input lists every required field in order",
			in:   wager.CommandInput{},
			code: wager.ValidationInvalidIdempotencyKey,
			want: []wager.FieldError{
				fe("providerId", "REQUIRED"),
				fe("externalTransactionId", "REQUIRED"),
				fe("idempotencyKey", "REQUIRED"),
				fe("playerId", "REQUIRED"),
				fe("walletId", "REQUIRED"),
				fe("roundId", "REQUIRED"),
				fe("gameId", "REQUIRED"),
				fe("kind", "REQUIRED"),
				fe("money.amount", "EMPTY"),
				fe("money.currency", "REQUIRED"),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := wager.NewCommand(tc.in)
			assertInvalid(t, got, err, tc.code, tc.want)
		})
	}
}

func TestNewCommand_NeverTrims(t *testing.T) {
	fields := []struct {
		name  string
		field func(*wager.CommandInput) *string
	}{
		{"providerId", func(in *wager.CommandInput) *string { return &in.ProviderID }},
		{"externalTransactionId", func(in *wager.CommandInput) *string { return &in.ExternalTransactionID }},
		{"idempotencyKey", func(in *wager.CommandInput) *string { return &in.IdempotencyKey }},
		{"playerId", func(in *wager.CommandInput) *string { return &in.PlayerID }},
		{"walletId", func(in *wager.CommandInput) *string { return &in.WalletID }},
		{"roundId", func(in *wager.CommandInput) *string { return &in.RoundID }},
		{"gameId", func(in *wager.CommandInput) *string { return &in.GameID }},
		{"kind", func(in *wager.CommandInput) *string { return &in.Kind }},
		{"money.amount", func(in *wager.CommandInput) *string { return &in.Amount }},
		{"money.currency", func(in *wager.CommandInput) *string { return &in.Currency }},
	}
	wraps := map[string]func(string) string{
		"leading space":      func(s string) string { return " " + s },
		"trailing space":     func(s string) string { return s + " " },
		"surrounding breaks": func(s string) string { return "\t" + s + "\n" },
	}
	for _, f := range fields {
		for wrapName, wrap := range wraps {
			t.Run(f.name+"/"+wrapName, func(t *testing.T) {
				in := betInput()
				value := f.field(&in)
				*value = wrap(*value)
				if _, err := wager.NewCommand(in); err == nil {
					t.Errorf("NewCommand() with %s = %q succeeded, want an error", f.name, *value)
				}
			})
		}
	}
}

func TestValidationError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  *wager.ValidationError
		want string
	}{
		{
			name: "code and fields",
			err: &wager.ValidationError{
				Code:   wager.ValidationInvalidMoney,
				Fields: []wager.FieldError{fe("kind", "UNSUPPORTED"), fe("money.amount", "SCALE_EXCEEDED")},
			},
			want: "wager: invalid command: INVALID_MONEY (kind: UNSUPPORTED, money.amount: SCALE_EXCEEDED)",
		},
		{
			name: "single field",
			err: &wager.ValidationError{
				Code:   wager.ValidationInvalidRequest,
				Fields: []wager.FieldError{fe("providerId", "REQUIRED")},
			},
			want: "wager: invalid command: INVALID_REQUEST (providerId: REQUIRED)",
		},
		{
			name: "no fields",
			err:  &wager.ValidationError{Code: wager.ValidationInvalidRequest},
			want: "wager: invalid command: INVALID_REQUEST",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Error(); got != tc.want {
				t.Errorf("Error() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestValidationError_DoesNotEchoInput(t *testing.T) {
	in := wager.CommandInput{
		ProviderID:                     "LEAK provider",
		ExternalTransactionID:          "LEAK transaction",
		IdempotencyKey:                 "LEAK key",
		PlayerID:                       "LEAK player",
		WalletID:                       "LEAK wallet",
		RoundID:                        "LEAK round",
		GameID:                         "LEAK game",
		Kind:                           "LEAK kind",
		Amount:                         "LEAK amount",
		Currency:                       "LEAK currency",
		ReferenceExternalTransactionID: "LEAK reference",
	}
	_, err := wager.NewCommand(in)
	if err == nil {
		t.Fatal("NewCommand() error = nil, want a validation error")
	}
	if msg := err.Error(); strings.Contains(msg, "LEAK") {
		t.Errorf("Error() = %q, must not contain input values", msg)
	}
}
