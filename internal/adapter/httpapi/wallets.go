package httpapi

import (
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wallets"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
)

const (
	codeInvalidMoney        = "INVALID_MONEY"
	codeUnsupportedCurrency = "UNSUPPORTED_CURRENCY"
	codeInvalidCursor       = "INVALID_CURSOR"
	codeWalletNotFound      = "WALLET_NOT_FOUND"
	codeWalletAlreadyExists = "WALLET_ALREADY_EXISTS"
)

const (
	fieldRequired      = "REQUIRED"
	fieldInvalidFormat = "INVALID_FORMAT"
	fieldUnsupported   = "UNSUPPORTED"
	fieldNotAString    = "NOT_A_STRING"
)

const (
	defaultLedgerLimit = 50
	maxLedgerLimit     = 200
)

var limitPattern = regexp.MustCompile(`^[1-9][0-9]{0,2}$`)

func (h *handlers) registerWallets(mux routeMux) {
	internal := func(f http.HandlerFunc) http.Handler { return h.authenticate(h.requireInternal(f)) }
	mux.Handle("POST /wallets", internal(h.openWallet))
	mux.Handle("GET /wallets/{walletId}", internal(h.getWallet))
	mux.Handle("GET /wallets/{walletId}/ledger", internal(h.walletLedger))
	mux.Handle("POST /wallets/{walletId}/reconciliation", internal(h.reconcileWallet))
}

func (h *handlers) openWallet(w http.ResponseWriter, r *http.Request) {
	var req openWalletRequest
	if err := decodeJSON(r, h.deps.MaxBodyBytes, &req); err != nil {
		var de *decodeError
		if errors.As(err, &de) && de.Field == "initialBalance.amount" {
			writeProblem(w, r, http.StatusBadRequest, codeInvalidMoney, "Invalid money", "amount must be a decimal string",
				[]wager.FieldError{{Field: de.Field, Code: fieldNotAString}})
			return
		}
		writeDecodeError(w, r, err)
		return
	}
	cmd, invalid := parseOpenWallet(req)
	if invalid != nil {
		writeProblem(w, r, http.StatusBadRequest, invalid.code, invalid.title, "", invalid.fields)
		return
	}
	cmd.CorrelationID = logging.FieldsFrom(r.Context()).CorrelationID

	snap, err := h.deps.Wallets.Open(r.Context(), cmd)
	if err != nil {
		h.writeWalletError(w, r, err)
		return
	}
	w.Header().Set("Location", "/wallets/"+snap.ID.String())
	writeJSON(w, http.StatusCreated, openWalletResponse{ID: snap.ID, PlayerID: snap.PlayerID, Balance: snap.Balance, Version: snap.Version})
}

type walletExistsProblem struct {
	problem
	WalletID string `json:"walletId,omitempty"`
}

type requestProblem struct {
	code, title string
	fields      []wager.FieldError
}

func parseOpenWallet(req openWalletRequest) (wallets.OpenCommand, *requestProblem) {
	var fields []wager.FieldError
	code, title := "", ""
	worse := func(c, t string) {
		if code == "" || c == codeInvalidMoney || (c == codeUnsupportedCurrency && code == codeInvalidRequest) {
			code, title = c, t
		}
	}

	player, ok := parseID(req.PlayerID)
	if !ok {
		fields = append(fields, wager.FieldError{Field: "playerId", Code: requiredOr(req.PlayerID, fieldInvalidFormat)})
		worse(codeInvalidRequest, "Invalid request")
	}
	initial, err := money.Parse(req.InitialBalance.Amount, req.InitialBalance.Currency)
	var parseErr *money.ParseError
	if errors.As(err, &parseErr) {
		fields = append(fields, wager.FieldError{Field: "initialBalance.amount", Code: string(parseErr.Reason)})
		worse(codeInvalidMoney, "Invalid money")
	}
	if errors.Is(err, money.ErrUnsupportedCurrency) {
		fields = append(fields, wager.FieldError{Field: "initialBalance.currency", Code: requiredOr(req.InitialBalance.Currency, fieldUnsupported)})
		worse(codeUnsupportedCurrency, "Unsupported currency")
	}
	if err != nil && parseErr == nil && !errors.Is(err, money.ErrUnsupportedCurrency) {
		fields = append(fields, wager.FieldError{Field: "initialBalance.amount", Code: string(money.ReasonMalformed)})
		worse(codeInvalidMoney, "Invalid money")
	}
	if len(fields) > 0 {
		return wallets.OpenCommand{}, &requestProblem{code: code, title: title, fields: fields}
	}
	return wallets.OpenCommand{PlayerID: player, Initial: initial}, nil
}

func requiredOr(value, code string) string {
	if value == "" {
		return fieldRequired
	}
	return code
}

func parseID(s string) (uuid.UUID, bool) {
	id, err := uuid.Parse(s)
	if err != nil || id == uuid.Nil || id.String() != s {
		return uuid.Nil, false
	}
	return id, true
}

func walletID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, ok := parseID(r.PathValue("walletId"))
	if !ok {
		writeProblem(w, r, http.StatusBadRequest, codeInvalidRequest, "Invalid request", "walletId must be a canonical UUID",
			[]wager.FieldError{{Field: "walletId", Code: fieldInvalidFormat}})
	}
	return id, ok
}

func (h *handlers) getWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := walletID(w, r)
	if !ok {
		return
	}
	snap, err := h.deps.Wallets.Get(r.Context(), id)
	if err != nil {
		h.writeWalletError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newWalletResponse(snap))
}

func (h *handlers) walletLedger(w http.ResponseWriter, r *http.Request) {
	id, ok := walletID(w, r)
	if !ok {
		return
	}
	afterSeq, limit, field := ledgerQuery(r)
	if field != "" {
		writeProblem(w, r, http.StatusBadRequest, codeInvalidCursor, "Invalid cursor",
			"cursor must be one this API returned and limit an integer from 1 to 200",
			[]wager.FieldError{{Field: field, Code: fieldInvalidFormat}})
		return
	}
	page, err := h.deps.Wallets.Ledger(r.Context(), id, afterSeq, limit)
	if err != nil {
		h.writeWalletError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newLedgerResponse(page))
}

func ledgerQuery(r *http.Request) (afterSeq int64, limit int, field string) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return 0, 0, malformedParam(r.URL.RawQuery)
	}
	limit = defaultLedgerLimit
	if values, given := q["limit"]; given {
		if len(values) != 1 || !limitPattern.MatchString(values[0]) {
			return 0, 0, "limit"
		}
		n, err := strconv.Atoi(values[0])
		if err != nil || n > maxLedgerLimit {
			return 0, 0, "limit"
		}
		limit = n
	}
	if values, given := q["cursor"]; given {
		if len(values) != 1 {
			return 0, 0, "cursor"
		}
		seq, err := decodeCursor(values[0])
		if err != nil {
			return 0, 0, "cursor"
		}
		afterSeq = seq
	}
	return afterSeq, limit, ""
}

func malformedParam(rawQuery string) string {
	for pair := range strings.SplitSeq(rawQuery, "&") {
		if _, err := url.ParseQuery(pair); err == nil {
			continue
		}
		switch key, _, _ := strings.Cut(pair, "="); key {
		case "limit", "cursor":
			return key
		}
		return "query"
	}
	return "query"
}

func (h *handlers) reconcileWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := walletID(w, r)
	if !ok {
		return
	}
	rec, err := h.deps.Wallets.Reconcile(r.Context(), id)
	if err != nil {
		h.writeWalletError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newReconciliationResponse(rec))
}

func (h *handlers) writeWalletError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, wallets.ErrWalletNotFound):
		writeProblem(w, r, http.StatusNotFound, codeWalletNotFound, "Wallet not found", "", nil)
	case errors.Is(err, wallets.ErrWalletAlreadyExists):
		p := walletExistsProblem{problem: newProblem(w, r, http.StatusConflict, codeWalletAlreadyExists,
			"Wallet already exists", "the player already has a wallet in this currency", nil)}
		var exists *wallets.AlreadyExistsError
		if errors.As(err, &exists) {
			p.WalletID = exists.WalletID.String()
		}
		sendProblem(w, p)
	case errors.Is(err, app.ErrUnavailable):
		h.deps.Log.WarnContext(r.Context(), "wallet request unavailable", slog.String("error", err.Error()))
		writeProblem(w, r, http.StatusServiceUnavailable, codeTemporarilyUnavailable, "Temporarily unavailable", "", nil)
	default:
		h.deps.Log.ErrorContext(r.Context(), "wallet request failed", slog.String("error", err.Error()))
		writeProblem(w, r, http.StatusInternalServerError, codeInternalError, "Internal error", "", nil)
	}
}
