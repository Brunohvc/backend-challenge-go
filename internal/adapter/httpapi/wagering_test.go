package httpapi

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
)

var exampleTx = uuid.MustParse("0192f298-345e-7e38-af88-e43f851a819d")

const providerBToken = "token-provider-b"

var wageringPrincipals = func() map[string]oidc.Principal {
	p := maps.Clone(principals)
	p[providerBToken] = oidc.Principal{ProviderID: "provider-b", Scopes: []string{oidc.ScopeWageringWrite, oidc.ScopeWageringRead}}
	return p
}()

type fakeWagering struct {
	calls int

	cmd    wager.Command
	src    wagering.Source
	result wagering.Result
	err    error

	getID       uuid.UUID
	getProvider string
	getExternal string
	getMethod   string
	snap        wager.Snapshot
	getErr      error
}

func (f *fakeWagering) Submit(_ context.Context, cmd wager.Command, src wagering.Source) (wagering.Result, error) {
	f.calls++
	f.cmd, f.src = cmd, src
	return f.result, f.err
}

func (f *fakeWagering) Get(_ context.Context, id uuid.UUID) (wager.Snapshot, error) {
	f.calls++
	f.getMethod, f.getID = "Get", id
	return f.snap, f.getErr
}

func (f *fakeWagering) GetForProvider(_ context.Context, id uuid.UUID, providerID string) (wager.Snapshot, error) {
	f.calls++
	f.getMethod, f.getID, f.getProvider = "GetForProvider", id, providerID
	return f.snap, f.getErr
}

func (f *fakeWagering) GetByExternal(_ context.Context, providerID, externalID string) (wager.Snapshot, error) {
	f.calls++
	f.getMethod, f.getProvider, f.getExternal = "GetByExternal", providerID, externalID
	return f.snap, f.getErr
}

type wageringAPI struct {
	handler http.Handler
	svc     *fakeWagering
	logs    *bytes.Buffer
}

func newWageringAPI(t *testing.T) *wageringAPI {
	t.Helper()
	logs := &bytes.Buffer{}
	svc := &fakeWagering{}
	h := NewRouter(Deps{
		Verifier:     fakeVerifier{principals: wageringPrincipals},
		Health:       fakeHealth{ready: true},
		Metrics:      &fakeMetrics{},
		Log:          logging.New(logs, -4, "test"),
		MaxBodyBytes: 65536,
		IDs:          fixedIDs{},
		Wallets:      &fakeWallets{},
		Wagering:     svc,
	})
	return &wageringAPI{handler: h, svc: svc, logs: logs}
}

func (a *wageringAPI) do(t *testing.T, method, target, body, token string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := request(t, method, target, body, token)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return a.send(req)
}

func request(t *testing.T, method, target, body, token string) *http.Request {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(headerCorrelationID, "c-wagering")
	return req
}

func (a *wageringAPI) send(req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	a.handler.ServeHTTP(rec, req)
	return rec
}

const betBody = `{
  "providerId": "provider-a",
  "externalTransactionId": "transaction-123",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "roundId": "round-987",
  "gameId": "fortune-chimp",
  "kind": "BET",
  "money": { "amount": "25.00", "currency": "BRL" }
}`

var withKey = map[string]string{"Idempotency-Key": "provider-a:transaction-123"}

func (a *wageringAPI) submit(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	return a.do(t, http.MethodPost, "/wagering/transactions", body, providerToken, withKey)
}

func mustMoney(amount string) *money.Money {
	m := mustBRL(amount)
	return &m
}

func snapshot(status wager.Status, code wager.FailureCode, balance *money.Money) wager.Snapshot {
	now := created
	s := wager.Snapshot{
		ID: exampleTx, Origin: wager.OriginExternal, Kind: wager.KindBet, Status: status,
		WalletID: exampleWallet, PlayerID: examplePlayer, Money: mustBRL("25.00"),
		ProviderID: "provider-a", ExternalTransactionID: "transaction-123", IdempotencyKey: "provider-a:transaction-123",
		RoundID: "round-987", GameID: "fortune-chimp", FailureCode: code, CorrelationID: "c-1",
		CreatedAt: now, UpdatedAt: now,
	}
	if balance != nil {
		s.ResultBalance, s.ResultWalletVersion = balance, 2
	}
	if status != wager.StatusPendingReference {
		s.ProcessedAt = &now
	}
	return s
}

func TestSubmit_Outcomes(t *testing.T) {
	tests := []struct {
		name     string
		result   wagering.Result
		status   int
		location bool
		body     string
	}{
		{"processed", wagering.Result{Transaction: snapshot(wager.StatusProcessed, "", mustMoney("975.00"))},
			http.StatusCreated, false,
			`{"transactionId":"0192f298-345e-7e38-af88-e43f851a819d","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}`},
		{"processed replay", wagering.Result{Transaction: snapshot(wager.StatusProcessed, "", mustMoney("975.00")), Replay: true},
			http.StatusOK, false,
			`{"transactionId":"0192f298-345e-7e38-af88-e43f851a819d","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":true}`},
		{"pending reference", wagering.Result{Transaction: snapshot(wager.StatusPendingReference, "", nil)},
			http.StatusAccepted, true,
			`{"transactionId":"0192f298-345e-7e38-af88-e43f851a819d","status":"PENDING_REFERENCE","idempotentReplay":false}`},
		{"pending reference replay", wagering.Result{Transaction: snapshot(wager.StatusPendingReference, "", nil), Replay: true},
			http.StatusAccepted, true,
			`{"transactionId":"0192f298-345e-7e38-af88-e43f851a819d","status":"PENDING_REFERENCE","idempotentReplay":true}`},
		{"rejected with a balance", wagering.Result{Transaction: snapshot(wager.StatusRejected, wager.FailureInsufficientFunds, mustMoney("20.00"))},
			http.StatusUnprocessableEntity, false,
			`{"transactionId":"0192f298-345e-7e38-af88-e43f851a819d","status":"REJECTED","failureCode":"INSUFFICIENT_FUNDS","balance":{"amount":"20.00","currency":"BRL"},"idempotentReplay":false}`},
		{"rejected replay", wagering.Result{Transaction: snapshot(wager.StatusRejected, wager.FailureInsufficientFunds, mustMoney("20.00")), Replay: true},
			http.StatusUnprocessableEntity, false,
			`{"transactionId":"0192f298-345e-7e38-af88-e43f851a819d","status":"REJECTED","failureCode":"INSUFFICIENT_FUNDS","balance":{"amount":"20.00","currency":"BRL"},"idempotentReplay":true}`},
		{"failed", wagering.Result{Transaction: snapshot(wager.StatusFailed, wager.FailureProcessingFailed, nil), Replay: true},
			http.StatusInternalServerError, false,
			`{"transactionId":"0192f298-345e-7e38-af88-e43f851a819d","status":"FAILED","failureCode":"PROCESSING_FAILED","idempotentReplay":true}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newWageringAPI(t)
			api.svc.result = tt.result

			rec := api.submit(t, betBody)

			requireJSON(t, rec, tt.status, tt.body)
			loc := rec.Header().Get("Location")
			if tt.location && loc != "/wagering/transactions/"+exampleTx.String() {
				t.Errorf("Location = %q, want /wagering/transactions/%s", loc, exampleTx)
			}
			if !tt.location && loc != "" {
				t.Errorf("Location = %q, want none", loc)
			}
			if strings.Contains(api.logs.String(), `"level":"ERROR"`) {
				t.Errorf("the outcome was logged as an error:\n%s", api.logs)
			}
		})
	}
}

func TestSubmit_UnexpectedStatusIsLogged(t *testing.T) {
	api := newWageringAPI(t)
	api.svc.result = wagering.Result{Transaction: snapshot(wager.StatusPending, "", nil)}

	rec := api.submit(t, betBody)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body %s)", rec.Code, rec.Body)
	}
	line := logLine(t, api.logs.String(), "wagering submission in an unexpected status")
	if line["level"] != "ERROR" || line["status"] != "PENDING" || line["transactionId"] != exampleTx.String() {
		t.Errorf("log = %v, want an error naming the status and the transaction", line)
	}
}

func TestSubmit_PassesTheCommand(t *testing.T) {
	api := newWageringAPI(t)
	api.svc.result = wagering.Result{Transaction: snapshot(wager.StatusProcessed, "", mustMoney("975.00"))}

	api.submit(t, betBody)

	cmd := api.svc.cmd
	if cmd.ProviderID != "provider-a" || cmd.ExternalTransactionID != "transaction-123" || cmd.IdempotencyKey != "provider-a:transaction-123" ||
		cmd.WalletID != exampleWallet || cmd.PlayerID != examplePlayer || cmd.Kind != wager.KindBet || cmd.Money.String() != "25.00" {
		t.Errorf("command = %+v, want the body's fields and the header's key", cmd)
	}
	if api.svc.src.Channel != wagering.ChannelHTTP || api.svc.src.CorrelationID != "c-wagering" || api.svc.src.Inbox != nil {
		t.Errorf("source = %+v, want HTTP with the request's correlation id", api.svc.src)
	}
}

func TestSubmit_ServiceErrors(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"key reused", wagering.ErrIdempotencyKeyReused, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED"},
		{"external id under another key", wagering.ErrDuplicateExternalTransaction, http.StatusConflict, "DUPLICATE_EXTERNAL_TRANSACTION"},
		{"wallet not found", wagering.ErrWalletNotFound, http.StatusNotFound, "WALLET_NOT_FOUND"},
		{"unavailable", app.ErrUnavailable, http.StatusServiceUnavailable, codeTemporarilyUnavailable},
		{"permanent", app.ErrPermanent, http.StatusInternalServerError, codeInternalError},
		{"unknown", fmt.Errorf("boom"), http.StatusInternalServerError, codeInternalError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newWageringAPI(t)
			api.svc.err = fmt.Errorf("wagering: submit: %w: secret-detail", tt.err)

			rec := api.submit(t, betBody)

			requireProblem(t, rec, tt.status, tt.code)
			if got := rec.Header().Get("Retry-After"); tt.status == http.StatusServiceUnavailable && got != "2" {
				t.Errorf("Retry-After = %q on 503, want 2", got)
			}
			if strings.Contains(rec.Body.String(), "secret-detail") {
				t.Error("the service error reached the response body")
			}
		})
	}
}

func TestSubmit_ReplayKeepsOriginalBalance(t *testing.T) {
	api := newWageringAPI(t)
	api.svc.result = wagering.Result{Transaction: snapshot(wager.StatusProcessed, "", mustMoney("975.00")), Replay: true}

	rec := api.submit(t, betBody)

	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"balance":{"amount":"975.00","currency":"BRL"}`) ||
		!strings.Contains(rec.Body.String(), `"idempotentReplay":true`) {
		t.Errorf("replay = %d %s, want 200 with the stored 975.00 and idempotentReplay true", rec.Code, rec.Body)
	}
}

func TestSubmit_RejectedWithoutBalance(t *testing.T) {
	for _, code := range []wager.FailureCode{wager.FailureCurrencyMismatch, wager.FailureWalletPlayerMismatch} {
		t.Run(string(code), func(t *testing.T) {
			api := newWageringAPI(t)
			api.svc.result = wagering.Result{Transaction: snapshot(wager.StatusRejected, code, nil)}

			rec := api.submit(t, betBody)

			requireJSON(t, rec, http.StatusUnprocessableEntity,
				`{"transactionId":"0192f298-345e-7e38-af88-e43f851a819d","status":"REJECTED","failureCode":"`+string(code)+`","idempotentReplay":false}`)
		})
	}
}

func TestSubmit_RejectedWithZeroBalanceKeepsIt(t *testing.T) {
	api := newWageringAPI(t)
	api.svc.result = wagering.Result{Transaction: snapshot(wager.StatusRejected, wager.FailureInsufficientFunds, mustMoney("0.00"))}

	rec := api.submit(t, betBody)

	if !strings.Contains(rec.Body.String(), `"balance":{"amount":"0.00","currency":"BRL"}`) {
		t.Errorf("body = %s, want the zero balance present", rec.Body)
	}
}

func TestSubmit_ProviderMismatchDoesNotCallService(t *testing.T) {
	api := newWageringAPI(t)

	rec := api.do(t, http.MethodPost, "/wagering/transactions", betBody, providerBToken, withKey)

	requireProblem(t, rec, http.StatusForbidden, "PROVIDER_MISMATCH")
	if api.svc.calls != 0 {
		t.Error("the service was called for another provider's command")
	}
}

func TestSubmit_Authorization(t *testing.T) {
	tests := []struct {
		name, token string
		status      int
		code        string
	}{
		{"no token", "", http.StatusUnauthorized, codeUnauthenticated},
		{"provider without wagering:write", readOnlyToken, http.StatusForbidden, codeForbidden},
		{"internal client", internalToken, http.StatusForbidden, codeForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newWageringAPI(t)

			rec := api.do(t, http.MethodPost, "/wagering/transactions", betBody, tt.token, withKey)

			requireProblem(t, rec, tt.status, tt.code)
			if api.svc.calls != 0 {
				t.Error("the service was called without authorization")
			}
		})
	}
}

func TestSubmit_MissingIdempotencyKey(t *testing.T) {
	api := newWageringAPI(t)

	for name, headers := range map[string]map[string]string{
		"absent": nil,
		"empty":  {"Idempotency-Key": ""},
	} {
		t.Run(name, func(t *testing.T) {
			rec := api.do(t, http.MethodPost, "/wagering/transactions", betBody, providerToken, headers)

			requireProblem(t, rec, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED")
		})
	}
	if api.svc.calls != 0 {
		t.Error("the service was called without a key")
	}
}

func TestSubmit_RepeatedIdempotencyKey(t *testing.T) {
	for name, keys := range map[string][]string{
		"two keys":           {"provider-a:transaction-123", "provider-a:transaction-124"},
		"an empty one first": {"", "provider-a:transaction-123"},
	} {
		t.Run(name, func(t *testing.T) {
			api := newWageringAPI(t)
			req := request(t, http.MethodPost, "/wagering/transactions", betBody, providerToken)
			for _, k := range keys {
				req.Header.Add(headerIdempotencyKey, k)
			}

			rec := api.send(req)

			p := requireProblem(t, rec, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY")
			requireFields(t, p, "idempotencyKey:INVALID_FORMAT")
			if api.svc.calls != 0 {
				t.Error("the service was called with a repeated key")
			}
		})
	}
}

func TestSubmit_InvalidIdempotencyKey(t *testing.T) {
	api := newWageringAPI(t)

	rec := api.do(t, http.MethodPost, "/wagering/transactions", betBody, providerToken, map[string]string{"Idempotency-Key": "has space"})

	p := requireProblem(t, rec, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY")
	requireFields(t, p, "idempotencyKey:INVALID_FORMAT")
}

func TestSubmit_ValidationErrors(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		code   string
		fields []string
	}{
		{"opening from outside", strings.Replace(betBody, `"BET"`, `"OPENING"`, 1), "INVALID_KIND", []string{"kind:UNSUPPORTED"}},
		{"amount without scale", strings.Replace(betBody, `"25.00"`, `"25"`, 1), "INVALID_MONEY", []string{"money.amount:MISSING_SCALE"}},
		{"amount as a JSON number", strings.Replace(betBody, `"25.00"`, `25.00`, 1), "INVALID_MONEY", []string{"money.amount:NOT_A_STRING"}},
		{"lowercase currency", strings.Replace(betBody, `"BRL"`, `"brl"`, 1), "UNSUPPORTED_CURRENCY", []string{"money.currency:UNSUPPORTED"}},
		{"zero bet", strings.Replace(betBody, `"25.00"`, `"0.00"`, 1), "INVALID_AMOUNT_FOR_KIND", []string{"money.amount:MUST_BE_POSITIVE"}},
		{"refund without reference", strings.Replace(betBody, `"BET"`, `"REFUND"`, 1), "REFERENCE_REQUIRED", []string{"referenceExternalTransactionId:REQUIRED"}},
		{"uppercase wallet id", strings.Replace(betBody, exampleWallet.String(), strings.ToUpper(exampleWallet.String()), 1), codeInvalidRequest, []string{"walletId:INVALID_FORMAT"}},
		{"unknown field", strings.Replace(betBody, `"kind"`, `"extra": 1, "kind"`, 1), codeInvalidRequest, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newWageringAPI(t)

			rec := api.submit(t, tt.body)

			p := requireProblem(t, rec, http.StatusBadRequest, tt.code)
			if tt.fields != nil {
				requireFields(t, p, tt.fields...)
			}
			if api.svc.calls != 0 {
				t.Error("the service was called with an invalid command")
			}
		})
	}
}

func TestSubmit_OpeningKindRejected(t *testing.T) {
	api := newWageringAPI(t)

	rec := api.submit(t, strings.Replace(betBody, `"BET"`, `"OPENING"`, 1))

	requireProblem(t, rec, http.StatusBadRequest, "INVALID_KIND")
	if api.svc.calls != 0 {
		t.Error("an OPENING from outside reached the service")
	}
}

const fullView = `{
	"transactionId": "0192f298-345e-7e38-af88-e43f851a819d",
	"status": "PENDING_REFERENCE",
	"kind": "REFUND",
	"providerId": "provider-a",
	"externalTransactionId": "transaction-123",
	"walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
	"playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
	"roundId": "round-987",
	"gameId": "fortune-chimp",
	"money": {"amount": "25.00", "currency": "BRL"},
	"referenceExternalTransactionId": "bet-1",
	"attempts": 2,
	"nextAttemptAt": "2026-09-08T12:00:04.000Z",
	"expiresAt": "2026-09-08T12:15:00.000Z",
	"createdAt": "2026-09-08T12:00:00.000Z",
	"updatedAt": "2026-09-08T12:00:00.000Z"
}`

func pendingRefund() wager.Snapshot {
	s := snapshot(wager.StatusPendingReference, "", nil)
	s.Kind = wager.KindRefund
	s.ReferenceExternalTransactionID = "bet-1"
	s.ReferenceAttempts = 2
	next, expires := created.Add(4*time.Second), created.Add(15*time.Minute)
	s.NextAttemptAt, s.ExpiresAt = &next, &expires
	return s
}

func TestGetTransaction(t *testing.T) {
	tests := []struct {
		name, token, method string
	}{
		{"provider reads its own", providerToken, "GetForProvider"},
		{"internal reads any", internalToken, "Get"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newWageringAPI(t)
			api.svc.snap = pendingRefund()

			rec := api.do(t, http.MethodGet, "/wagering/transactions/"+exampleTx.String(), "", tt.token, nil)

			requireJSON(t, rec, http.StatusOK, fullView)
			if api.svc.getMethod != tt.method || api.svc.getID != exampleTx {
				t.Errorf("service call = %s(%s), want %s(%s)", api.svc.getMethod, api.svc.getID, tt.method, exampleTx)
			}
			if tt.method == "GetForProvider" && api.svc.getProvider != "provider-a" {
				t.Errorf("provider = %q, want the token's provider-a", api.svc.getProvider)
			}
		})
	}
}

func TestGetTransaction_ProcessedView(t *testing.T) {
	api := newWageringAPI(t)
	s := snapshot(wager.StatusProcessed, "", mustMoney("975.00"))
	ref := uuid.MustParse("0192f2c0-0000-7000-8000-000000000001")
	s.Kind, s.ReferenceExternalTransactionID, s.ReferenceTransactionID = wager.KindWin, "bet-1", uuid.NullUUID{UUID: ref, Valid: true}
	api.svc.snap = s

	rec := api.do(t, http.MethodGet, "/wagering/transactions/"+exampleTx.String(), "", providerToken, nil)

	for _, want := range []string{
		`"balance":{"amount":"975.00","currency":"BRL"}`, `"walletVersion":2`,
		`"referenceTransactionId":"0192f2c0-0000-7000-8000-000000000001"`, `"processedAt":"2026-09-08T12:00:00.000Z"`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("body = %s, want %s", rec.Body, want)
		}
	}
	if strings.Contains(rec.Body.String(), "nextAttemptAt") || strings.Contains(rec.Body.String(), "failureCode") {
		t.Errorf("body = %s, want no schedule nor failure code on a processed transaction", rec.Body)
	}
}

func TestGetTransaction_OtherProvider404(t *testing.T) {
	api := newWageringAPI(t)
	api.svc.getErr = fmt.Errorf("x: %w", wagering.ErrTransactionNotFound)

	rec := api.do(t, http.MethodGet, "/wagering/transactions/"+exampleTx.String(), "", providerBToken, nil)

	requireProblem(t, rec, http.StatusNotFound, "TRANSACTION_NOT_FOUND")
	if api.svc.getMethod != "GetForProvider" || api.svc.getProvider != "provider-b" {
		t.Errorf("service call = %s for %q, want GetForProvider scoped to provider-b", api.svc.getMethod, api.svc.getProvider)
	}
}

func TestGetTransaction_AccessAndInput(t *testing.T) {
	tests := []struct {
		name, target, token string
		status              int
		code                string
	}{
		{"no token", "/wagering/transactions/" + exampleTx.String(), "", http.StatusUnauthorized, codeUnauthenticated},
		{"provider without wagering:read", "/wagering/transactions/" + exampleTx.String(), writeOnlyToken, http.StatusForbidden, codeForbidden},
		{"invalid id", "/wagering/transactions/not-a-uuid", providerToken, http.StatusBadRequest, codeInvalidRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newWageringAPI(t)

			rec := api.do(t, http.MethodGet, tt.target, "", tt.token, nil)

			requireProblem(t, rec, tt.status, tt.code)
			if api.svc.calls != 0 {
				t.Error("the service was called")
			}
		})
	}
}

func TestGetByExternal(t *testing.T) {
	tests := []struct {
		name, token string
	}{
		{"provider reads its own", providerToken},
		{"internal reads any provider's", internalToken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newWageringAPI(t)
			api.svc.snap = pendingRefund()

			rec := api.do(t, http.MethodGet, "/providers/provider-a/wagering/transactions/transaction-123", "", tt.token, nil)

			requireJSON(t, rec, http.StatusOK, fullView)
			if api.svc.getMethod != "GetByExternal" || api.svc.getProvider != "provider-a" || api.svc.getExternal != "transaction-123" {
				t.Errorf("service call = %s(%q, %q)", api.svc.getMethod, api.svc.getProvider, api.svc.getExternal)
			}
		})
	}
}

func TestGetByExternal_Access(t *testing.T) {
	tests := []struct {
		name, token string
		status      int
		code        string
	}{
		{"no token", "", http.StatusUnauthorized, codeUnauthenticated},
		{"provider without wagering:read", writeOnlyToken, http.StatusForbidden, codeForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newWageringAPI(t)

			rec := api.do(t, http.MethodGet, "/providers/provider-a/wagering/transactions/transaction-123", "", tt.token, nil)

			requireProblem(t, rec, tt.status, tt.code)
			if api.svc.calls != 0 {
				t.Error("the service was called")
			}
		})
	}
}

func TestGetByExternal_OtherProvider403(t *testing.T) {
	api := newWageringAPI(t)

	rec := api.do(t, http.MethodGet, "/providers/provider-a/wagering/transactions/transaction-123", "", providerBToken, nil)

	requireProblem(t, rec, http.StatusForbidden, "PROVIDER_MISMATCH")
	if api.svc.calls != 0 {
		t.Error("the service was called for another provider's path")
	}
}

func TestGetByExternal_RefusesAPrincipalOfNeitherRole(t *testing.T) {
	tests := []struct {
		name       string
		principal  *oidc.Principal
		providerID string
	}{
		{"no principal", nil, "provider-a"},
		{
			"bound to no provider and without wallets:manage",
			&oidc.Principal{Scopes: []string{oidc.ScopeWageringRead}},
			"provider-a",
		},
		{
			"bound to a provider and holding wallets:manage",
			&oidc.Principal{ProviderID: "provider-a", Scopes: []string{oidc.ScopeWageringRead, oidc.ScopeWalletsManage}},
			"provider-b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeWagering{snap: pendingRefund()}
			h := newHandlers(Deps{Wagering: svc, Log: logging.New(&bytes.Buffer{}, -4, "test")})
			ctx := t.Context()
			if tt.principal != nil {
				ctx = oidc.WithPrincipal(ctx, *tt.principal)
			}
			req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/providers/"+tt.providerID+"/wagering/transactions/transaction-123", nil)
			req.SetPathValue("providerId", tt.providerID)
			req.SetPathValue("externalTransactionId", "transaction-123")
			rec := httptest.NewRecorder()

			h.getTransactionByExternal(rec, req)

			requireProblem(t, rec, http.StatusForbidden, "PROVIDER_MISMATCH")
			if svc.calls != 0 {
				t.Error("the service was called")
			}
		})
	}
}

func TestGetByExternal_NotFoundAndInput(t *testing.T) {
	api := newWageringAPI(t)
	api.svc.getErr = wagering.ErrTransactionNotFound

	rec := api.do(t, http.MethodGet, "/providers/provider-a/wagering/transactions/transaction-404", "", providerToken, nil)
	requireProblem(t, rec, http.StatusNotFound, "TRANSACTION_NOT_FOUND")

	rec = api.do(t, http.MethodGet, "/providers/provider-a/wagering/transactions/bad%20id", "", internalToken, nil)
	p := requireProblem(t, rec, http.StatusBadRequest, codeInvalidRequest)
	requireFields(t, p, "externalTransactionId:INVALID_FORMAT")
}

func TestValidationTitles_EveryCode(t *testing.T) {
	for _, code := range []wager.ValidationCode{
		wager.ValidationInvalidIdempotencyKey,
		wager.ValidationInvalidKind,
		wager.ValidationInvalidMoney,
		wager.ValidationUnsupportedCurrency,
		wager.ValidationInvalidAmountForKind,
		wager.ValidationReferenceRequired,
		wager.ValidationInvalidRequest,
	} {
		if validationTitles[code] == "" {
			t.Errorf("no title for %s", code)
		}
	}
}

func TestValidationTitle_FallsBack(t *testing.T) {
	if got := validationTitle(wager.ValidationInvalidKind); got != "Invalid kind" {
		t.Errorf("validationTitle(INVALID_KIND) = %q, want its own title", got)
	}
	if got := validationTitle("A_CODE_ADDED_LATER"); got != "Invalid request" {
		t.Errorf("validationTitle(an unknown code) = %q, want the generic Invalid request", got)
	}
}
