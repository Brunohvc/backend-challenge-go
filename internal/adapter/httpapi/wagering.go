package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
)

const (
	codeIdempotencyKeyRequired       = "IDEMPOTENCY_KEY_REQUIRED"
	codeProviderMismatch             = "PROVIDER_MISMATCH"
	codeTransactionNotFound          = "TRANSACTION_NOT_FOUND"
	codeIdempotencyKeyReused         = "IDEMPOTENCY_KEY_REUSED"
	codeDuplicateExternalTransaction = "DUPLICATE_EXTERNAL_TRANSACTION"
)

const headerIdempotencyKey = "Idempotency-Key"

var validationTitles = map[wager.ValidationCode]string{
	wager.ValidationInvalidIdempotencyKey: "Invalid idempotency key",
	wager.ValidationInvalidKind:           "Invalid kind",
	wager.ValidationInvalidMoney:          "Invalid money",
	wager.ValidationUnsupportedCurrency:   "Unsupported currency",
	wager.ValidationInvalidAmountForKind:  "Invalid amount for kind",
	wager.ValidationReferenceRequired:     "Reference required",
	wager.ValidationInvalidRequest:        "Invalid request",
}

func validationTitle(code wager.ValidationCode) string {
	if title, ok := validationTitles[code]; ok {
		return title
	}
	return validationTitles[wager.ValidationInvalidRequest]
}

func (h *handlers) registerWagering(mux routeMux) {
	mux.Handle("POST /wagering/transactions",
		h.authenticate(h.requireProvider(oidc.ScopeWageringWrite, http.HandlerFunc(h.submitTransaction))))
	mux.Handle("GET /wagering/transactions/{transactionId}",
		h.authenticate(h.requireReader(oidc.ScopeWageringRead, http.HandlerFunc(h.getTransaction))))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}",
		h.authenticate(h.requireReader(oidc.ScopeWageringRead, http.HandlerFunc(h.getTransactionByExternal))))
}

func (h *handlers) submitTransaction(w http.ResponseWriter, r *http.Request) {
	keys := r.Header.Values(headerIdempotencyKey)
	if len(keys) > 1 {
		writeProblem(w, r, http.StatusBadRequest, string(wager.ValidationInvalidIdempotencyKey), "Invalid idempotency key",
			"send a single Idempotency-Key header", []wager.FieldError{{Field: "idempotencyKey", Code: fieldInvalidFormat}})
		return
	}
	if len(keys) == 0 || keys[0] == "" {
		writeProblem(w, r, http.StatusBadRequest, codeIdempotencyKeyRequired, "Idempotency key required",
			"the Idempotency-Key header is required", nil)
		return
	}

	var req submitRequest
	if err := decodeJSON(r, h.deps.MaxBodyBytes, &req); err != nil {
		var de *decodeError
		if errors.As(err, &de) && de.Field == "money.amount" {
			writeProblem(w, r, http.StatusBadRequest, codeInvalidMoney, "Invalid money", "amount must be a decimal string",
				[]wager.FieldError{{Field: de.Field, Code: fieldNotAString}})
			return
		}
		writeDecodeError(w, r, err)
		return
	}
	cmd, err := wager.NewCommand(req.commandInput(keys[0]))
	if err != nil {
		var ve *wager.ValidationError
		if errors.As(err, &ve) {
			writeProblem(w, r, http.StatusBadRequest, string(ve.Code), validationTitle(ve.Code), "", ve.Fields)
			return
		}
		h.writeWageringError(w, r, err)
		return
	}
	if p, _ := oidc.PrincipalFrom(r.Context()); cmd.ProviderID != p.ProviderID {
		writeProblem(w, r, http.StatusForbidden, codeProviderMismatch, "Provider mismatch",
			"providerId does not match the access token", nil)
		return
	}

	res, err := h.deps.Wagering.Submit(r.Context(), cmd, wagering.Source{
		Channel:       wagering.ChannelHTTP,
		CorrelationID: logging.FieldsFrom(r.Context()).CorrelationID,
	})
	if err != nil {
		h.writeWageringError(w, r, err)
		return
	}
	tx := res.Transaction
	status := h.submitStatus(r.Context(), tx, res.Replay)
	if tx.Status == wager.StatusPendingReference {
		w.Header().Set("Location", "/wagering/transactions/"+tx.ID.String())
	}
	writeJSON(w, status, newSubmitResponse(tx, res.Replay))
}

func (h *handlers) submitStatus(ctx context.Context, tx wager.Snapshot, replay bool) int {
	switch tx.Status {
	case wager.StatusProcessed:
		if replay {
			return http.StatusOK
		}
		return http.StatusCreated
	case wager.StatusPendingReference:
		return http.StatusAccepted
	case wager.StatusRejected:
		return http.StatusUnprocessableEntity
	case wager.StatusFailed:
		return http.StatusInternalServerError
	default:
		h.deps.Log.ErrorContext(ctx, "wagering submission in an unexpected status",
			slog.String("status", string(tx.Status)), slog.String("transactionId", tx.ID.String()))
		return http.StatusInternalServerError
	}
}

func (h *handlers) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(r.PathValue("transactionId"))
	if !ok {
		writeProblem(w, r, http.StatusBadRequest, codeInvalidRequest, "Invalid request", "transactionId must be a canonical UUID",
			[]wager.FieldError{{Field: "transactionId", Code: fieldInvalidFormat}})
		return
	}
	var snap wager.Snapshot
	var err error
	if p, _ := oidc.PrincipalFrom(r.Context()); p.IsInternal() {
		snap, err = h.deps.Wagering.Get(r.Context(), id)
	} else {
		snap, err = h.deps.Wagering.GetForProvider(r.Context(), id, p.ProviderID)
	}
	if err != nil {
		h.writeWageringError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newTransactionResponse(snap))
}

func (h *handlers) getTransactionByExternal(w http.ResponseWriter, r *http.Request) {
	providerID, externalID := r.PathValue("providerId"), r.PathValue("externalTransactionId")
	if p, _ := oidc.PrincipalFrom(r.Context()); !p.IsInternal() && providerID != p.ProviderID {
		writeProblem(w, r, http.StatusForbidden, codeProviderMismatch, "Provider mismatch",
			"providerId does not match the access token", nil)
		return
	}
	var fields []wager.FieldError
	if !wager.ValidTextID(providerID) {
		fields = append(fields, wager.FieldError{Field: "providerId", Code: fieldInvalidFormat})
	}
	if !wager.ValidTextID(externalID) {
		fields = append(fields, wager.FieldError{Field: "externalTransactionId", Code: fieldInvalidFormat})
	}
	if fields != nil {
		writeProblem(w, r, http.StatusBadRequest, codeInvalidRequest, "Invalid request", "", fields)
		return
	}
	snap, err := h.deps.Wagering.GetByExternal(r.Context(), providerID, externalID)
	if err != nil {
		h.writeWageringError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newTransactionResponse(snap))
}

func (h *handlers) writeWageringError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, wagering.ErrIdempotencyKeyReused):
		writeProblem(w, r, http.StatusConflict, codeIdempotencyKeyReused, "Idempotency key reused",
			"key already used with a different payload", nil)
	case errors.Is(err, wagering.ErrDuplicateExternalTransaction):
		writeProblem(w, r, http.StatusConflict, codeDuplicateExternalTransaction, "Duplicate external transaction",
			"externalTransactionId already submitted under another idempotency key", nil)
	case errors.Is(err, wagering.ErrWalletNotFound):
		writeProblem(w, r, http.StatusNotFound, codeWalletNotFound, "Wallet not found", "", nil)
	case errors.Is(err, wagering.ErrTransactionNotFound):
		writeProblem(w, r, http.StatusNotFound, codeTransactionNotFound, "Transaction not found", "", nil)
	case errors.Is(err, app.ErrUnavailable):
		h.deps.Log.WarnContext(r.Context(), "wagering request unavailable", slog.String("error", err.Error()))
		writeProblem(w, r, http.StatusServiceUnavailable, codeTemporarilyUnavailable, "Temporarily unavailable", "", nil)
	default:
		h.deps.Log.ErrorContext(r.Context(), "wagering request failed", slog.String("error", err.Error()))
		writeProblem(w, r, http.StatusInternalServerError, codeInternalError, "Internal error", "", nil)
	}
}
