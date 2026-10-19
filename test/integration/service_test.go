//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/httpapi"
	"github.com/Brunohvc/backend-challenge-go/internal/bootstrap"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

const (
	codeUnauthenticated     = "UNAUTHENTICATED"
	codeForbidden           = "FORBIDDEN"
	codeProviderMismatch    = "PROVIDER_MISMATCH"
	codeTransactionNotFound = "TRANSACTION_NOT_FOUND"
)

type service struct {
	base   string
	client *http.Client
}

func startService(t *testing.T, overrides map[string]string) *service {
	t.Helper()
	cfg := env.ServiceConfig(t, "integration", overrides)
	logs := &testenv.SyncBuffer{}
	var api *httpapi.Server
	app := fxtest.New(t,
		bootstrap.Module(cfg),
		fx.Replace(logging.New(logs, slog.LevelDebug, cfg.InstanceID)),
		fx.Populate(&api),
	)
	app.RequireStart()
	s := &service{
		base:   "http://" + api.Addr(),
		client: &http.Client{Transport: &http.Transport{}, Timeout: 10 * time.Second},
	}
	t.Cleanup(func() {
		s.client.CloseIdleConnections()
		app.RequireStop()
		if t.Failed() {
			t.Logf("service log:\n%s", logs.String())
		}
	})
	return s
}

type call struct {
	method, path   string
	authorization  string
	idempotencyKey string
	body           any
}

func (c call) String() string { return c.method + " " + c.path }

func (c call) as(tok string) call {
	c.authorization = bearer(tok)
	return c
}

func bearer(tok string) string { return "Bearer " + tok }

type reply struct {
	call   call
	status int
	header http.Header
	body   []byte
}

func (s *service) send(t *testing.T, c call) reply {
	t.Helper()
	var body io.Reader
	if c.body != nil {
		raw, err := json.Marshal(c.body)
		if err != nil {
			t.Fatalf("%s: encode body: %v", c, err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(t.Context(), c.method, s.base+c.path, body)
	if err != nil {
		t.Fatalf("%s: new request: %v", c, err)
	}
	if c.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.authorization != "" {
		req.Header.Set("Authorization", c.authorization)
	}
	if c.idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", c.idempotencyKey)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatalf("%s: %v", c, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s: read body: %v", c, err)
	}
	return reply{call: c, status: resp.StatusCode, header: resp.Header, body: raw}
}

func (r reply) json(t *testing.T) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(r.body, &v); err != nil {
		t.Fatalf("%s = %d: decode %q: %v", r.call, r.status, r.body, err)
	}
	return v
}

func (r reply) decode(t *testing.T, status int, v any) {
	t.Helper()
	r.want(t, status)
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("%s = %d: decode %q: %v", r.call, r.status, r.body, err)
	}
}

func (r reply) want(t *testing.T, status int) map[string]any {
	t.Helper()
	if r.status != status {
		t.Fatalf("%s = %d %s, want %d", r.call, r.status, r.body, status)
	}
	return r.json(t)
}

var problemFields = []string{"type", "title", "status", "code", "detail", "correlationId", "errors"}

func (r reply) wantProblem(t *testing.T, status int, code string) map[string]any {
	t.Helper()
	if r.status != status {
		t.Fatalf("%s = %d %s, want %d %s", r.call, r.status, r.body, status, code)
	}
	if ct := r.header.Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("%s: Content-Type = %q, want application/problem+json", r.call, ct)
	}
	p := r.json(t)
	if p["code"] != code {
		t.Errorf("%s: code = %v, want %s (%s)", r.call, p["code"], code, r.body)
	}
	for _, name := range slices.Sorted(maps.Keys(p)) {
		if !slices.Contains(problemFields, name) {
			t.Errorf("%s: problem carries %q, which is not a problem member: %s", r.call, name, r.body)
		}
	}
	return p
}

func newID() string { return idgen.V7{}.New().String() }

func openWalletCall(internal, player string) call {
	return call{
		method: http.MethodPost, path: "/wallets", authorization: bearer(internal),
		body: map[string]any{
			"playerId":       player,
			"initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"},
		},
	}
}

type wallet struct {
	id, player string
}

func (s *service) openWallet(t *testing.T, internal string) wallet {
	t.Helper()
	player := newID()
	got := s.send(t, openWalletCall(internal, player)).want(t, http.StatusCreated)
	id, _ := got["id"].(string)
	return wallet{id: id, player: player}
}

func betCall(tok, providerID string, w wallet) call {
	unique := newID()
	return call{
		method: http.MethodPost, path: "/wagering/transactions", authorization: bearer(tok),
		idempotencyKey: "key-" + unique,
		body: map[string]any{
			"providerId":            providerID,
			"externalTransactionId": "bet-" + unique,
			"playerId":              w.player,
			"walletId":              w.id,
			"roundId":               "round-" + unique,
			"gameId":                "game-1",
			"kind":                  "BET",
			"money":                 map[string]string{"amount": "10.00", "currency": "BRL"},
		},
	}
}

func (c call) externalID() string {
	body, _ := c.body.(map[string]any)
	id, _ := body["externalTransactionId"].(string)
	return id
}

type placedBet struct {
	call call
	id   string
}

func (b placedBet) byIDCall(tok string) call {
	return call{method: http.MethodGet, path: "/wagering/transactions/" + b.id, authorization: bearer(tok)}
}

func (b placedBet) byExternalCall(tok, providerID string) call {
	return call{
		method:        http.MethodGet,
		path:          "/providers/" + providerID + "/wagering/transactions/" + b.call.externalID(),
		authorization: bearer(tok),
	}
}

func (s *service) placeBet(t *testing.T, tok, providerID string, w wallet) placedBet {
	t.Helper()
	c := betCall(tok, providerID, w)
	got := s.send(t, c).want(t, http.StatusCreated)
	if got["status"] != "PROCESSED" {
		t.Fatalf("%s = %v, want PROCESSED", c, got)
	}
	id, _ := got["transactionId"].(string)
	return placedBet{call: c, id: id}
}

func (b placedBet) reversalCall(tok, providerID, kind string) call {
	unique := newID()
	body := withProvider(b.call.body, providerID)
	body["externalTransactionId"] = "reversal-" + unique
	body["kind"] = kind
	body["referenceExternalTransactionId"] = b.call.externalID()
	return call{
		method: http.MethodPost, path: "/wagering/transactions", authorization: bearer(tok),
		idempotencyKey: "key-" + unique, body: body,
	}
}

func walletCalls(tok string, w wallet) []call {
	return []call{
		openWalletCall(tok, newID()),
		{method: http.MethodGet, path: "/wallets/" + w.id, authorization: bearer(tok)},
		{method: http.MethodGet, path: "/wallets/" + w.id + "/ledger", authorization: bearer(tok)},
		{method: http.MethodPost, path: "/wallets/" + w.id + "/reconciliation", authorization: bearer(tok)},
	}
}

type footprint struct {
	wallets, transactions, ledgerEntries, outboxEvents, inboxMessages int64
	wallet                                                            string
}

func takeFootprint(t *testing.T, w wallet) footprint {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), env.PG.OwnerDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(t.Context()) }()
	var f footprint
	err = conn.QueryRow(t.Context(), `
SELECT (SELECT count(*) FROM wallets),
       (SELECT count(*) FROM wager_transactions),
       (SELECT count(*) FROM wallet_ledger_entries),
       (SELECT count(*) FROM outbox_events),
       (SELECT count(*) FROM inbox_messages),
       (SELECT w::text FROM wallets w WHERE w.id = $1)`, w.id).
		Scan(&f.wallets, &f.transactions, &f.ledgerEntries, &f.outboxEvents, &f.inboxMessages, &f.wallet)
	if err != nil {
		t.Fatalf("read footprint: %v", err)
	}
	return f
}

func wantUnchanged(t *testing.T, w wallet, before footprint) {
	t.Helper()
	if after := takeFootprint(t, w); after != before {
		t.Errorf("database changed by refused requests:\n before %+v\n after  %+v", before, after)
	}
}
