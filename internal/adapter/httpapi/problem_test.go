package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
)

func TestWriteProblem_Shape(t *testing.T) {
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/wallets", nil)
	req = req.WithContext(logging.With(req.Context(), logging.Fields{CorrelationID: "c-1"}))
	rec := httptest.NewRecorder()

	writeProblem(rec, req, http.StatusBadRequest, "INVALID_MONEY", "Invalid money", "amount is not canonical",
		[]wager.FieldError{{Field: "initialBalance.amount", Code: "MISSING_SCALE"}})

	p := requireProblem(t, rec, http.StatusBadRequest, "INVALID_MONEY")
	if p["title"] != "Invalid money" || p["detail"] != "amount is not canonical" || p["correlationId"] != "c-1" {
		t.Errorf("problem = %v", p)
	}
	errs, _ := p["errors"].([]any)
	first, _ := errs[0].(map[string]any)
	if len(errs) != 1 || first["field"] != "initialBalance.amount" || first["code"] != "MISSING_SCALE" {
		t.Errorf("errors = %v, want one entry with field and code", p["errors"])
	}
}

func TestWriteProblem_OmitsEmptyParts(t *testing.T) {
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()

	writeProblem(rec, req, http.StatusNotFound, "WALLET_NOT_FOUND", "Wallet not found", "", nil)

	p := requireProblem(t, rec, http.StatusNotFound, "WALLET_NOT_FOUND")
	for _, k := range []string{"detail", "errors", "correlationId"} {
		if _, ok := p[k]; ok {
			t.Errorf("problem has %s although it is empty: %v", k, p)
		}
	}
}
