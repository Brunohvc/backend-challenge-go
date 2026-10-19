package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type openWalletBody struct {
	PlayerID       string `json:"playerId"`
	InitialBalance struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"initialBalance"`
}

func decode(t *testing.T, contentType, body string, maxBytes int64) *decodeError {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/wallets", strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	var dst openWalletBody
	err := decodeJSON(req, maxBytes, &dst)
	if err == nil {
		return nil
	}
	var de *decodeError
	if !errors.As(err, &de) {
		t.Fatalf("err = %v (%T), want *decodeError", err, err)
	}
	return de
}

const validBody = `{"playerId":"p","initialBalance":{"amount":"10.00","currency":"BRL"}}`

func TestDecode_Valid(t *testing.T) {
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/wallets", strings.NewReader(validBody))
	req.Header.Set("Content-Type", "application/json")
	var dst openWalletBody

	if err := decodeJSON(req, 1024, &dst); err != nil {
		t.Fatalf("decodeJSON: %v", err)
	}

	if dst.PlayerID != "p" || dst.InitialBalance.Amount != "10.00" || dst.InitialBalance.Currency != "BRL" {
		t.Errorf("decoded = %+v", dst)
	}
}

func TestDecode_ContentType(t *testing.T) {
	tests := []struct {
		contentType string
		wantStatus  int
	}{
		{"", http.StatusUnsupportedMediaType},
		{"text/plain", http.StatusUnsupportedMediaType},
		{"application/jsonx", http.StatusUnsupportedMediaType},
		{"application/json; charset", http.StatusUnsupportedMediaType},
		{"application/json", 0},
		{"application/json; charset=utf-8", 0},
		{"Application/JSON", 0},
	}
	for _, tt := range tests {
		t.Run(tt.contentType, func(t *testing.T) {
			de := decode(t, tt.contentType, validBody, 1024)

			if tt.wantStatus == 0 {
				if de != nil {
					t.Fatalf("err = %v, want none", de)
				}
				return
			}
			if de == nil || de.Status != tt.wantStatus || de.Code != codeUnsupportedMediaType {
				t.Errorf("err = %+v, want status %d code %s", de, tt.wantStatus, codeUnsupportedMediaType)
			}
		})
	}
}

func TestDecode_TooLarge(t *testing.T) {
	body := `{"playerId":"` + strings.Repeat("p", 2048) + `"}`

	de := decode(t, "application/json", body, 1024)

	if de == nil || de.Status != http.StatusRequestEntityTooLarge || de.Code != codePayloadTooLarge {
		t.Errorf("err = %+v, want status 413 code %s", de, codePayloadTooLarge)
	}
}

func TestDecode_TrailingData(t *testing.T) {
	for _, body := range []string{`{}{}`, `{} x`, `{} {"playerId":"p"}`, validBody + `]`} {
		t.Run(body, func(t *testing.T) {
			de := decode(t, "application/json", body, 1024)

			if de == nil || de.Status != http.StatusBadRequest || de.Code != codeInvalidRequest {
				t.Errorf("err = %+v, want 400 %s", de, codeInvalidRequest)
			}
		})
	}
}

func TestDecode_NotAnObject(t *testing.T) {
	for _, body := range []string{``, `   `, `null`, `[]`, `"text"`, `{"playerId":`} {
		t.Run(body, func(t *testing.T) {
			de := decode(t, "application/json", body, 1024)

			if de == nil || de.Status != http.StatusBadRequest || de.Code != codeInvalidRequest {
				t.Errorf("err = %+v, want 400 %s", de, codeInvalidRequest)
			}
		})
	}
}

func TestDecode_UnknownField(t *testing.T) {
	de := decode(t, "application/json", `{"playerId":"p","extra":1}`, 1024)

	if de == nil || de.Status != http.StatusBadRequest || de.Code != codeInvalidRequest || de.Field != "extra" {
		t.Errorf("err = %+v, want 400 %s on field extra", de, codeInvalidRequest)
	}
}

func TestDecode_WrongTypeNamesTheField(t *testing.T) {
	de := decode(t, "application/json", `{"playerId":"p","initialBalance":{"amount":25.00,"currency":"BRL"}}`, 1024)

	if de == nil || de.Status != http.StatusBadRequest || de.Field != "initialBalance.amount" {
		t.Errorf("err = %+v, want 400 on field initialBalance.amount", de)
	}
}
