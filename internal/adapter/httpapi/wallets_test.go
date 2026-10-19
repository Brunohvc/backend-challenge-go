package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wallets"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wallet"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
)

var (
	examplePlayer = uuid.MustParse("0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1")
	exampleWallet = uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef37")
	created       = time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
)

type ledgerArgs struct{ afterSeq, limit int64 }

type fakeWallets struct {
	calls int

	openCmd  wallets.OpenCommand
	openErr  error
	getErr   error
	ledgerQ  ledgerArgs
	ledgerID uuid.UUID
	getSnap  *wallet.Snapshot
	page     wallets.LedgerPage
	ledgerEr error
	rec      wallets.Reconciliation
	recErr   error
}

func (f *fakeWallets) Open(_ context.Context, cmd wallets.OpenCommand) (wallet.Snapshot, error) {
	f.calls++
	f.openCmd = cmd
	if f.openErr != nil {
		return wallet.Snapshot{}, f.openErr
	}
	return wallet.Snapshot{ID: exampleWallet, PlayerID: cmd.PlayerID, Balance: cmd.Initial, Version: 1, CreatedAt: created, UpdatedAt: created}, nil
}

func (f *fakeWallets) Get(_ context.Context, id uuid.UUID) (wallet.Snapshot, error) {
	f.calls++
	if f.getErr != nil {
		return wallet.Snapshot{}, f.getErr
	}
	if f.getSnap != nil {
		return *f.getSnap, nil
	}
	return wallet.Snapshot{ID: id, PlayerID: examplePlayer, Balance: mustBRL("975.00"), Version: 2, CreatedAt: created, UpdatedAt: created.Add(time.Minute)}, nil
}

func (f *fakeWallets) Ledger(_ context.Context, id uuid.UUID, afterSeq int64, limit int) (wallets.LedgerPage, error) {
	f.calls++
	f.ledgerID = id
	f.ledgerQ = ledgerArgs{afterSeq: afterSeq, limit: int64(limit)}
	return f.page, f.ledgerEr
}

func (f *fakeWallets) Reconcile(_ context.Context, id uuid.UUID) (wallets.Reconciliation, error) {
	f.calls++
	if f.recErr != nil {
		return wallets.Reconciliation{}, f.recErr
	}
	r := f.rec
	r.WalletID = id
	return r, nil
}

type walletAPI struct {
	handler http.Handler
	svc     *fakeWallets
	logs    *bytes.Buffer
}

func newWalletAPI(t *testing.T) *walletAPI {
	t.Helper()
	logs := &bytes.Buffer{}
	svc := &fakeWallets{}
	h := NewRouter(Deps{
		Verifier:     fakeVerifier{principals: principals},
		Health:       fakeHealth{ready: true},
		Metrics:      &fakeMetrics{},
		Log:          logging.New(logs, -4, "test"),
		MaxBodyBytes: 65536,
		IDs:          fixedIDs{},
		Wallets:      svc,
	})
	return &walletAPI{handler: h, svc: svc, logs: logs}
}

func (a *walletAPI) do(t *testing.T, method, target, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(headerCorrelationID, "c-wallets")
	rec := httptest.NewRecorder()
	a.handler.ServeHTTP(rec, req)
	return rec
}

const openBody = `{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}`

func TestOpenWallet_Created(t *testing.T) {
	api := newWalletAPI(t)

	rec := api.do(t, http.MethodPost, "/wallets", openBody, internalToken)

	requireJSON(t, rec, http.StatusCreated, `{
		"id": "0192f291-27dd-7d3f-8071-5f8685deef37",
		"playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		"balance": { "amount": "1000.00", "currency": "BRL" },
		"version": 1
	}`)
	if loc := rec.Header().Get("Location"); loc != "/wallets/"+exampleWallet.String() {
		t.Errorf("Location = %q, want /wallets/%s", loc, exampleWallet)
	}
	cmd := api.svc.openCmd
	if cmd.PlayerID != examplePlayer || cmd.Initial.String() != "1000.00" || cmd.Initial.Currency().Code() != "BRL" || cmd.CorrelationID != "c-wallets" {
		t.Errorf("command = %+v, want the body's player and balance and the request's correlation id", cmd)
	}
}

func TestOpenWallet_ZeroBalance(t *testing.T) {
	api := newWalletAPI(t)

	rec := api.do(t, http.MethodPost, "/wallets", strings.Replace(openBody, "1000.00", "0.00", 1), internalToken)

	if rec.Code != http.StatusCreated || !api.svc.openCmd.Initial.IsZero() {
		t.Errorf("status = %d, initial = %v; want 201 with a zero initial balance", rec.Code, api.svc.openCmd.Initial)
	}
}

func TestOpenWallet_Conflict(t *testing.T) {
	api := newWalletAPI(t)
	existing := uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef99")
	api.svc.openErr = fmt.Errorf("wrapped: %w", &wallets.AlreadyExistsError{WalletID: existing})

	rec := api.do(t, http.MethodPost, "/wallets", openBody, internalToken)

	p := requireProblem(t, rec, http.StatusConflict, "WALLET_ALREADY_EXISTS")
	if p["walletId"] != existing.String() {
		t.Errorf("walletId = %v, want %s", p["walletId"], existing)
	}
}

func TestOpenWallet_ConflictBody(t *testing.T) {
	api := newWalletAPI(t)
	existing := uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef99")
	api.svc.openErr = &wallets.AlreadyExistsError{WalletID: existing}

	rec := api.do(t, http.MethodPost, "/wallets", openBody, internalToken)

	const want = `{"type":"about:blank","title":"Wallet already exists","status":409,"code":"WALLET_ALREADY_EXISTS",` +
		`"detail":"the player already has a wallet in this currency","correlationId":"c-wallets",` +
		`"walletId":"0192f291-27dd-7d3f-8071-5f8685deef99"}` + "\n"
	if got := rec.Body.String(); got != want {
		t.Errorf("body =\n%s\nwant\n%s", got, want)
	}
}

func TestOpenWallet_ConflictWithoutTheWalletID(t *testing.T) {
	api := newWalletAPI(t)
	api.svc.openErr = fmt.Errorf("wrapped: %w", wallets.ErrWalletAlreadyExists)

	rec := api.do(t, http.MethodPost, "/wallets", openBody, internalToken)

	p := requireProblem(t, rec, http.StatusConflict, "WALLET_ALREADY_EXISTS")
	if _, ok := p["walletId"]; ok {
		t.Errorf("walletId = %v, want the member absent", p["walletId"])
	}
}

func TestOpenWallet_Unavailable(t *testing.T) {
	api := newWalletAPI(t)
	api.svc.openErr = fmt.Errorf("wallets: %w: contention", app.ErrUnavailable)

	rec := api.do(t, http.MethodPost, "/wallets", openBody, internalToken)

	requireProblem(t, rec, http.StatusServiceUnavailable, codeTemporarilyUnavailable)
	if rec.Header().Get("Retry-After") == "" {
		t.Error("no Retry-After on 503")
	}
}

func TestOpenWallet_InternalErrorHidesDetail(t *testing.T) {
	api := newWalletAPI(t)
	api.svc.openErr = fmt.Errorf("postgres: %w: secret-table-name", app.ErrPermanent)

	rec := api.do(t, http.MethodPost, "/wallets", openBody, internalToken)

	requireProblem(t, rec, http.StatusInternalServerError, codeInternalError)
	if strings.Contains(rec.Body.String(), "secret-table-name") {
		t.Error("the internal error reached the response body")
	}
	if !strings.Contains(api.logs.String(), "secret-table-name") {
		t.Error("the internal error was not logged")
	}
}

func TestOpenWallet_InvalidMoney(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		code   string
		fields []string
	}{
		{"amount without scale", strings.Replace(openBody, `"1000.00"`, `"25"`, 1),
			"INVALID_MONEY", []string{"initialBalance.amount:MISSING_SCALE"}},
		{"amount as a JSON number", strings.Replace(openBody, `"1000.00"`, `25.00`, 1),
			"INVALID_MONEY", []string{"initialBalance.amount:NOT_A_STRING"}},
		{"negative amount", strings.Replace(openBody, `"1000.00"`, `"-1.00"`, 1),
			"INVALID_MONEY", []string{"initialBalance.amount:SIGNED"}},
		{"lowercase currency", strings.Replace(openBody, `"BRL"`, `"brl"`, 1),
			"UNSUPPORTED_CURRENCY", []string{"initialBalance.currency:UNSUPPORTED"}},
		{"every field wrong", `{"playerId":"not-a-uuid","initialBalance":{"amount":"1e3","currency":"JPY"}}`,
			"INVALID_MONEY", []string{"playerId:INVALID_FORMAT", "initialBalance.amount:SCIENTIFIC_NOTATION", "initialBalance.currency:UNSUPPORTED"}},
		{"missing balance", `{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"}`,
			"INVALID_MONEY", []string{"initialBalance.amount:EMPTY", "initialBalance.currency:REQUIRED"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newWalletAPI(t)

			rec := api.do(t, http.MethodPost, "/wallets", tt.body, internalToken)

			p := requireProblem(t, rec, http.StatusBadRequest, tt.code)
			requireFields(t, p, tt.fields...)
			if api.svc.calls != 0 {
				t.Error("the service was called with an invalid body")
			}
		})
	}
}

func TestOpenWallet_InvalidPlayer(t *testing.T) {
	tests := []struct{ name, player, code string }{
		{"missing", ``, "REQUIRED"},
		{"uppercase", `0192F28F-5DC0-7D58-BDB2-814AD6A0F4A1`, "INVALID_FORMAT"},
		{"braces", `{0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1}`, "INVALID_FORMAT"},
		{"nil", `00000000-0000-0000-0000-000000000000`, "INVALID_FORMAT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newWalletAPI(t)
			body := strings.Replace(openBody, examplePlayer.String(), tt.player, 1)

			rec := api.do(t, http.MethodPost, "/wallets", body, internalToken)

			p := requireProblem(t, rec, http.StatusBadRequest, codeInvalidRequest)
			requireFields(t, p, "playerId:"+tt.code)
		})
	}
}

func TestOpenWallet_StrictBody(t *testing.T) {
	api := newWalletAPI(t)

	rec := api.do(t, http.MethodPost, "/wallets", strings.Replace(openBody, `"playerId"`, `"extra":1,"playerId"`, 1), internalToken)

	requireProblem(t, rec, http.StatusBadRequest, codeInvalidRequest)
	if api.svc.calls != 0 {
		t.Error("the service was called with an unknown field")
	}
}

func TestGetWallet(t *testing.T) {
	api := newWalletAPI(t)

	rec := api.do(t, http.MethodGet, "/wallets/"+exampleWallet.String(), "", internalToken)

	requireJSON(t, rec, http.StatusOK, `{
		"id": "0192f291-27dd-7d3f-8071-5f8685deef37",
		"playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		"balance": { "amount": "975.00", "currency": "BRL" },
		"version": 2,
		"createdAt": "2026-09-08T12:00:00.000Z",
		"updatedAt": "2026-09-08T12:01:00.000Z"
	}`)
}

func TestGetWallet_TimesInUTC(t *testing.T) {
	api := newWalletAPI(t)
	saoPaulo := time.FixedZone("BRT", -3*60*60)
	api.svc.getSnap = &wallet.Snapshot{
		ID: exampleWallet, PlayerID: examplePlayer, Balance: mustBRL("1.00"), Version: 1,
		CreatedAt: time.Date(2026, time.September, 8, 9, 0, 0, 123456789, saoPaulo),
		UpdatedAt: time.Date(2026, time.September, 8, 9, 30, 0, 0, saoPaulo),
	}

	rec := api.do(t, http.MethodGet, "/wallets/"+exampleWallet.String(), "", internalToken)

	if !strings.Contains(rec.Body.String(), `"createdAt":"2026-09-08T12:00:00.123Z"`) ||
		!strings.Contains(rec.Body.String(), `"updatedAt":"2026-09-08T12:30:00.000Z"`) {
		t.Errorf("body = %s, want both times in UTC with milliseconds", rec.Body)
	}
}

func TestGetWallet_NotFound(t *testing.T) {
	api := newWalletAPI(t)
	api.svc.getErr = fmt.Errorf("x: %w", wallets.ErrWalletNotFound)

	rec := api.do(t, http.MethodGet, "/wallets/"+exampleWallet.String(), "", internalToken)

	requireProblem(t, rec, http.StatusNotFound, "WALLET_NOT_FOUND")
}

func TestGetWallet_InvalidID(t *testing.T) {
	api := newWalletAPI(t)

	for _, id := range []string{"not-a-uuid", strings.ToUpper(exampleWallet.String()), uuid.Nil.String()} {
		rec := api.do(t, http.MethodGet, "/wallets/"+id, "", internalToken)

		p := requireProblem(t, rec, http.StatusBadRequest, codeInvalidRequest)
		requireFields(t, p, "walletId:INVALID_FORMAT")
	}
	if api.svc.calls != 0 {
		t.Error("the service was called with an invalid id")
	}
}

func ledgerRecord(seq int64, dir wallet.Direction, amount, before, after string) app.LedgerRecord {
	e, err := wallet.NewLedgerEntry(uuid.MustParse(fmt.Sprintf("0192f2a0-0000-7000-8000-%012d", seq)), exampleWallet,
		uuid.MustParse(fmt.Sprintf("0192f2b0-0000-7000-8000-%012d", seq)), dir, mustBRL(amount), mustBRL(before), mustBRL(after), created)
	if err != nil {
		panic(err)
	}
	return app.LedgerRecord{Seq: seq, Entry: e}
}

func TestLedger_Pagination(t *testing.T) {
	api := newWalletAPI(t)
	next := int64(7)
	api.svc.page = wallets.LedgerPage{
		Records: []app.LedgerRecord{
			ledgerRecord(3, wallet.DirectionCredit, "1000.00", "0.00", "1000.00"),
			ledgerRecord(7, wallet.DirectionDebit, "25.00", "1000.00", "975.00"),
		},
		NextAfterSeq: &next,
	}
	target := "/wallets/" + exampleWallet.String() + "/ledger?limit=2"

	rec := api.do(t, http.MethodGet, target, "", internalToken)

	requireJSON(t, rec, http.StatusOK, `{
		"items": [
			{"id": "0192f2a0-0000-7000-8000-000000000003", "transactionId": "0192f2b0-0000-7000-8000-000000000003",
			 "direction": "CREDIT", "amount": {"amount": "1000.00", "currency": "BRL"},
			 "balanceBefore": {"amount": "0.00", "currency": "BRL"}, "balanceAfter": {"amount": "1000.00", "currency": "BRL"},
			 "createdAt": "2026-09-08T12:00:00.000Z"},
			{"id": "0192f2a0-0000-7000-8000-000000000007", "transactionId": "0192f2b0-0000-7000-8000-000000000007",
			 "direction": "DEBIT", "amount": {"amount": "25.00", "currency": "BRL"},
			 "balanceBefore": {"amount": "1000.00", "currency": "BRL"}, "balanceAfter": {"amount": "975.00", "currency": "BRL"},
			 "createdAt": "2026-09-08T12:00:00.000Z"}
		],
		"nextCursor": "`+encodeCursor(7)+`"
	}`)
	if api.svc.ledgerQ != (ledgerArgs{afterSeq: 0, limit: 2}) || api.svc.ledgerID != exampleWallet {
		t.Errorf("first page asked as %v for wallet %s, want after 0, limit 2 for %s", api.svc.ledgerQ, api.svc.ledgerID, exampleWallet)
	}

	api.svc.page = wallets.LedgerPage{Records: []app.LedgerRecord{ledgerRecord(9, wallet.DirectionDebit, "5.00", "975.00", "970.00")}}
	rec = api.do(t, http.MethodGet, target+"&cursor="+encodeCursor(7), "", internalToken)

	var body struct {
		Items      []map[string]any `json:"items"`
		NextCursor *string          `json:"nextCursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("second page = %d %s", rec.Code, rec.Body)
	}
	if len(body.Items) != 1 || body.NextCursor != nil || !strings.Contains(rec.Body.String(), `"nextCursor":null`) {
		t.Errorf("last page = %s, want one item and nextCursor null", rec.Body)
	}
	if api.svc.ledgerQ != (ledgerArgs{afterSeq: 7, limit: 2}) {
		t.Errorf("second page asked as %v, want after 7, limit 2", api.svc.ledgerQ)
	}
}

func TestLedger_DefaultLimit(t *testing.T) {
	api := newWalletAPI(t)

	rec := api.do(t, http.MethodGet, "/wallets/"+exampleWallet.String()+"/ledger", "", internalToken)

	if rec.Code != http.StatusOK || api.svc.ledgerQ != (ledgerArgs{afterSeq: 0, limit: 50}) {
		t.Errorf("status = %d, asked %v; want 200 with after 0, limit 50", rec.Code, api.svc.ledgerQ)
	}
	if !strings.Contains(rec.Body.String(), `"items":[]`) {
		t.Errorf("empty ledger body = %s, want items as an empty array", rec.Body)
	}
}

func TestLedger_BadLimit(t *testing.T) {
	api := newWalletAPI(t)
	base := "/wallets/" + exampleWallet.String() + "/ledger?"

	tests := []struct{ query, field string }{
		{"limit=0", "limit"}, {"limit=201", "limit"}, {"limit=abc", "limit"}, {"limit=", "limit"},
		{"limit=+5", "limit"}, {"limit=05", "limit"}, {"limit=-1", "limit"}, {"limit=1&limit=2", "limit"},
		{"cursor=tampered", "cursor"}, {"cursor=", "cursor"},
		{"cursor=" + encodeCursor(1) + "&cursor=" + encodeCursor(2), "cursor"},
		{"cursor=%zz", "cursor"}, {"limit=%zz", "limit"}, {"limit=300%zz", "limit"},
		{"limit=5;cursor=x", "limit"},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			rec := api.do(t, http.MethodGet, base+tt.query, "", internalToken)

			p := requireProblem(t, rec, http.StatusBadRequest, "INVALID_CURSOR")
			requireFields(t, p, tt.field+":INVALID_FORMAT")
		})
	}
	if api.svc.calls != 0 {
		t.Error("the service was called with an invalid page")
	}
	if rec := api.do(t, http.MethodGet, base+"limit=200", "", internalToken); rec.Code != http.StatusOK {
		t.Errorf("limit=200: status %d, want 200", rec.Code)
	}
}

func TestLedger_UnknownWallet(t *testing.T) {
	api := newWalletAPI(t)
	api.svc.ledgerEr = wallets.ErrWalletNotFound

	rec := api.do(t, http.MethodGet, "/wallets/"+exampleWallet.String()+"/ledger", "", internalToken)

	requireProblem(t, rec, http.StatusNotFound, "WALLET_NOT_FOUND")
}

func TestReconcile_ResponseShape(t *testing.T) {
	api := newWalletAPI(t)
	api.svc.rec = wallets.Reconciliation{
		Stored: mustBRL("975.00"), Calculated: mustBRL("975.00"), Difference: mustBRL("0.00"),
		Consistent: true, CheckedEntries: 2,
	}

	rec := api.do(t, http.MethodPost, "/wallets/"+exampleWallet.String()+"/reconciliation", "", internalToken)

	requireJSON(t, rec, http.StatusOK, `{
		"walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
		"storedBalance": { "amount": "975.00", "currency": "BRL" },
		"calculatedBalance": { "amount": "975.00", "currency": "BRL" },
		"difference": { "amount": "0.00", "currency": "BRL" },
		"consistent": true,
		"checkedEntries": 2
	}`)
}

func TestReconcile_NotFound(t *testing.T) {
	api := newWalletAPI(t)
	api.svc.recErr = wallets.ErrWalletNotFound

	rec := api.do(t, http.MethodPost, "/wallets/"+exampleWallet.String()+"/reconciliation", "", internalToken)

	requireProblem(t, rec, http.StatusNotFound, "WALLET_NOT_FOUND")
}

func TestAuth_ProviderOnWalletRoute(t *testing.T) {
	routes := []struct{ method, target, body string }{
		{http.MethodPost, "/wallets", openBody},
		{http.MethodGet, "/wallets/" + exampleWallet.String(), ""},
		{http.MethodGet, "/wallets/" + exampleWallet.String() + "/ledger", ""},
		{http.MethodPost, "/wallets/" + exampleWallet.String() + "/reconciliation", ""},
	}
	for _, r := range routes {
		t.Run(r.method+" "+r.target, func(t *testing.T) {
			api := newWalletAPI(t)

			forbidden := api.do(t, r.method, r.target, r.body, providerToken)
			unauthenticated := api.do(t, r.method, r.target, r.body, "")

			requireProblem(t, forbidden, http.StatusForbidden, codeForbidden)
			requireProblem(t, unauthenticated, http.StatusUnauthorized, codeUnauthenticated)
			if api.svc.calls != 0 {
				t.Errorf("the service was called %d times without an internal principal", api.svc.calls)
			}
		})
	}
}
