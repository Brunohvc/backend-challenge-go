package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/goleak"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/money"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type fakeVerifier struct {
	principals map[string]oidc.Principal
	err        error
}

func (f fakeVerifier) Verify(_ context.Context, raw string) (oidc.Principal, error) {
	if p, ok := f.principals[raw]; ok {
		return p, nil
	}
	if f.err != nil {
		return oidc.Principal{}, f.err
	}
	return oidc.Principal{}, oidc.ErrInvalidToken
}

const (
	providerToken   = "token-provider-a"
	readOnlyToken   = "token-provider-a-read-only"
	writeOnlyToken  = "token-provider-a-write-only"
	internalToken   = "token-backoffice"
	rolelessToken   = "token-of-no-provider"
	generatedIDText = "0192f298-345e-7e38-af88-e43f851a819d"
)

var principals = map[string]oidc.Principal{
	providerToken:  {ProviderID: "provider-a", Scopes: []string{oidc.ScopeWageringWrite, oidc.ScopeWageringRead}},
	readOnlyToken:  {ProviderID: "provider-a", Scopes: []string{oidc.ScopeWageringRead}},
	writeOnlyToken: {ProviderID: "provider-a", Scopes: []string{oidc.ScopeWageringWrite}},
	internalToken:  {Scopes: []string{oidc.ScopeWalletsManage}},
	rolelessToken:  {Scopes: []string{oidc.ScopeWageringRead}},
}

type fixedIDs struct{}

func (fixedIDs) New() uuid.UUID { return uuid.MustParse(generatedIDText) }

type observation struct {
	route, method string
	code          int
}

type fakeMetrics struct {
	mu  sync.Mutex
	obs []observation
}

func (f *fakeMetrics) Observe(route, method string, code int, _ time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.obs = append(f.obs, observation{route, method, code})
}

func (f *fakeMetrics) last(t *testing.T) observation {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.obs) == 0 {
		t.Fatal("no request was observed")
	}
	return f.obs[len(f.obs)-1]
}

type fakeHealth struct {
	ready  bool
	checks map[string]string
}

func (f fakeHealth) Ready(context.Context) (bool, map[string]string) { return f.ready, f.checks }

type testAPI struct {
	handler http.Handler
	logs    *bytes.Buffer
	metrics *fakeMetrics
	reached map[string]bool
}

func newTestAPI(t *testing.T, mutate ...func(*Deps)) *testAPI {
	t.Helper()
	logs := &bytes.Buffer{}
	api := &testAPI{logs: logs, metrics: &fakeMetrics{}, reached: map[string]bool{}}
	d := Deps{
		Verifier:     fakeVerifier{principals: principals},
		Health:       fakeHealth{ready: true, checks: map[string]string{}},
		Metrics:      api.metrics,
		Log:          logging.New(logs, -4, "test"),
		MaxBodyBytes: 1024,
		IDs:          fixedIDs{},
	}
	for _, m := range mutate {
		m(&d)
	}
	h := newHandlers(d)
	mux := http.NewServeMux()
	h.registerHealth(mux)
	mark := func(name string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			api.reached[name] = true
			w.WriteHeader(http.StatusNoContent)
		})
	}
	mux.Handle("GET /test/protected", h.authenticate(mark("protected")))
	mux.Handle("GET /test/internal", h.authenticate(h.requireInternal(mark("internal"))))
	mux.Handle("GET /test/provider", h.authenticate(h.requireProvider(oidc.ScopeWageringWrite, mark("provider"))))
	mux.Handle("GET /test/reader", h.authenticate(h.requireReader(oidc.ScopeWageringRead, mark("reader"))))
	mux.Handle("POST /test/provider", h.authenticate(h.requireProvider(oidc.ScopeWageringWrite,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Money struct{ Amount, Currency string } `json:"money"`
			}
			if err := decodeJSON(r, d.MaxBodyBytes, &body); err != nil {
				writeDecodeError(w, r, err)
				return
			}
			api.reached["provider-post"] = true
			writeJSON(w, http.StatusCreated, map[string]any{"balance": body.Money})
		}))))
	mux.HandleFunc("GET /test/panic", func(http.ResponseWriter, *http.Request) { panic("boom") })
	api.handler = h.middleware(mux)
	return api
}

func (a *testAPI) do(t *testing.T, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	a.handler.ServeHTTP(rec, req)
	return rec
}

func bearer(token string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + token}
}

func problemOf(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q, want application/problem+json (body %s)", ct, rec.Body)
	}
	var p map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem %s: %v", rec.Body, err)
	}
	return p
}

func requireProblem(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) map[string]any {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, status, rec.Body)
	}
	p := problemOf(t, rec)
	if p["code"] != code || p["status"] != float64(status) || p["type"] != "about:blank" || p["title"] == "" {
		t.Fatalf("problem = %v, want type about:blank, a title, status %d and code %s", p, status, code)
	}
	return p
}

func mustBRL(amount string) money.Money {
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		panic(err)
	}
	return m
}

func requireJSON(t *testing.T, rec *httptest.ResponseRecorder, status int, want string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, status, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got, exp any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body %s: %v", rec.Body, err)
	}
	if err := json.Unmarshal([]byte(want), &exp); err != nil {
		t.Fatalf("decode want: %v", err)
	}
	gotJSON, _ := json.Marshal(got)
	expJSON, _ := json.Marshal(exp)
	if !bytes.Equal(gotJSON, expJSON) {
		t.Errorf("body =\n%s\nwant\n%s", gotJSON, expJSON)
	}
}

func requireFields(t *testing.T, p map[string]any, want ...string) {
	t.Helper()
	errs, _ := p["errors"].([]any)
	got := make([]string, 0, len(errs))
	for _, e := range errs {
		m, _ := e.(map[string]any)
		got = append(got, fmt.Sprintf("%v:%v", m["field"], m["code"]))
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("errors = %v, want %v", got, want)
	}
}
