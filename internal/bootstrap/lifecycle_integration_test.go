//go:build integration

package bootstrap_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/goleak"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/httpapi"
	"github.com/Brunohvc/backend-challenge-go/internal/bootstrap"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/metrics"
	"github.com/Brunohvc/backend-challenge-go/test/testenv"
)

var env *testenv.Env

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	e, err := testenv.Start(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() {
		if err := e.Terminate(context.Background()); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()
	env = e
	return m.Run()
}

var consumerTimings = map[string]string{
	"CONSUMER_WAIT_TIME":          "1s",
	"CONSUMER_VISIBILITY_TIMEOUT": "8s",
	"CONSUMER_PROCESSING_TIMEOUT": "6s",
}

func appConfig(t *testing.T, overrides map[string]string) config.Config {
	t.Helper()
	settings := maps.Clone(consumerTimings)
	maps.Copy(settings, overrides)
	vars := env.AppEnv("bootstrap-it", settings)
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if _, set := vars[name]; set || !strings.HasPrefix(name, "AWS_") {
			continue
		}
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
	for name, value := range vars {
		if strings.HasPrefix(name, "AWS_") {
			t.Setenv(name, value)
		}
	}
	cfg, err := config.Load(func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	})
	if err != nil {
		t.Fatalf("config.Load() error = %v", err)
	}
	return cfg
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) records(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var recs []map[string]any
	dec := json.NewDecoder(bytes.NewReader(b.buf.Bytes()))
	for dec.More() {
		var r map[string]any
		if err := dec.Decode(&r); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		recs = append(recs, r)
	}
	return recs
}

func (b *syncBuffer) components(t *testing.T, msg string) []string {
	t.Helper()
	var names []string
	for _, r := range b.records(t) {
		if r["msg"] == msg {
			name, _ := r["component"].(string)
			names = append(names, name)
		}
	}
	return names
}

type testApp struct {
	*fxtest.App
	logs  *syncBuffer
	api   *httpapi.Server
	admin *metrics.AdminServer
}

func newApp(t *testing.T, cfg config.Config) *testApp {
	t.Helper()
	a := &testApp{logs: &syncBuffer{}}
	opts := []fx.Option{
		bootstrap.Module(cfg),
		fx.Replace(logging.New(a.logs, slog.LevelInfo, cfg.InstanceID)),
		fx.Populate(&a.admin),
	}
	if cfg.HTTP.Enabled {
		opts = append(opts, fx.Populate(&a.api))
	}
	a.App = fxtest.New(t, opts...)
	t.Cleanup(func() {
		if t.Failed() {
			a.logs.mu.Lock()
			defer a.logs.mu.Unlock()
			t.Logf("process log:\n%s", a.logs.buf.String())
		}
	})
	return a
}

type client struct {
	t    *testing.T
	http *http.Client
	base string
}

func newClient(t *testing.T, base string) *client {
	return &client{t: t, http: &http.Client{Transport: &http.Transport{}, Timeout: 10 * time.Second}, base: base}
}

func (c *client) request(method, path string, body io.Reader) *http.Request {
	c.t.Helper()
	req, err := http.NewRequestWithContext(c.t.Context(), method, c.base+path, body)
	if err != nil {
		c.t.Fatalf("new request: %v", err)
	}
	return req
}

func (c *client) send(req *http.Request) (int, []byte) {
	c.t.Helper()
	resp, err := c.http.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatalf("%s %s: read body: %v", req.Method, req.URL.Path, err)
	}
	return resp.StatusCode, raw
}

func (c *client) get(path string) (int, string) {
	c.t.Helper()
	code, raw := c.send(c.request(http.MethodGet, path, nil))
	return code, string(raw)
}

func (c *client) call(method, path, token string, body any, headers map[string]string) (int, map[string]any) {
	c.t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		c.t.Fatalf("encode body: %v", err)
	}
	req := c.request(method, path, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	code, raw := c.send(req)
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		c.t.Fatalf("%s %s = %d: decode %q: %v", method, path, code, raw, err)
	}
	return code, decoded
}

func token(t *testing.T, clientID, secret string) string {
	t.Helper()
	tok, err := env.KC.Token(t.Context(), clientID, secret)
	if err != nil {
		t.Fatalf("token for %s: %v", clientID, err)
	}
	return tok
}

func TestApp_StartStop_NoLeaks(t *testing.T) {
	internal := token(t, "wallet-service", "wallet-service-secret")
	provider := token(t, "provider-a", "provider-a-secret")
	before := goleak.IgnoreCurrent()

	app := newApp(t, appConfig(t, nil))
	app.RequireStart()
	api := newClient(t, "http://"+app.api.Addr())
	admin := newClient(t, "http://"+app.admin.Addr())

	if code, body := api.get("/health/ready"); code != http.StatusOK ||
		!strings.Contains(body, `"postgres":"ok"`) || !strings.Contains(body, `"sqs":"ok"`) {
		t.Fatalf("GET /health/ready = %d %s, want 200 with postgres and sqs ok", code, body)
	}
	player := idgen.V7{}.New().String()
	code, wallet := api.call(http.MethodPost, "/wallets", internal, map[string]any{
		"playerId":       player,
		"initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"},
	}, nil)
	if code != http.StatusCreated {
		t.Fatalf("POST /wallets = %d %v, want 201", code, wallet)
	}
	code, bet := api.call(http.MethodPost, "/wagering/transactions", provider, map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": "bet-" + player,
		"playerId":              player,
		"walletId":              wallet["id"],
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  "BET",
		"money":                 map[string]string{"amount": "25.00", "currency": "BRL"},
	}, map[string]string{"Idempotency-Key": "key-" + player})
	if code != http.StatusCreated || bet["status"] != "PROCESSED" {
		t.Fatalf("POST /wagering/transactions = %d %v, want 201 PROCESSED", code, bet)
	}
	if balance, _ := bet["balance"].(map[string]any); balance["amount"] != "75.00" {
		t.Errorf("balance after the bet = %v, want 75.00", bet["balance"])
	}
	if code, body := admin.get("/metrics"); code != http.StatusOK ||
		!strings.Contains(body, `wagering_transactions_total{channel="http",failure_code="",kind="BET",status="PROCESSED"} 1`) {
		t.Fatalf("GET /metrics = %d, want 200 counting the bet:\n%s", code, body)
	}

	app.RequireStop()

	api.http.CloseIdleConnections()
	admin.http.CloseIdleConnections()
	verifyNoLeaks(t, before)
}

func verifyNoLeaks(t *testing.T, before goleak.Option) {
	t.Helper()
	shared, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("http.DefaultTransport is a %T, want *http.Transport", http.DefaultTransport)
	}
	shared.CloseIdleConnections()
	goleak.VerifyNone(t, before,
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)
}

func TestApp_FailsFastOnBadDeps(t *testing.T) {
	wrongPassword, err := url.Parse(env.PG.AppDSN)
	if err != nil {
		t.Fatal(err)
	}
	wrongPassword.User = url.UserPassword("wallet_app", "not-the-password-s3cr3t")
	notMigrated, err := url.Parse(env.PG.AppDSN)
	if err != nil {
		t.Fatal(err)
	}
	notMigrated.Path = "/postgres"
	var lc net.ListenConfig
	taken, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = taken.Close() })

	tests := []struct {
		name      string
		overrides map[string]string
		want      string
		failed    string
		stopped   []string
	}{
		{
			name:      "admin address in use",
			overrides: map[string]string{"ADMIN_ADDR": taken.Addr().String()},
			want:      "listen on " + taken.Addr().String(),
			failed:    "admin",
			stopped:   nil,
		},
		{
			name:      "wrong database password",
			overrides: map[string]string{"DATABASE_URL": wrongPassword.String()},
			want:      "postgres: connect",
			failed:    "postgres",
			stopped:   []string{"admin"},
		},
		{
			name:      "database never migrated",
			overrides: map[string]string{"DATABASE_URL": notMigrated.String()},
			want:      "not migrated",
			failed:    "postgres",
			stopped:   []string{"admin"},
		},
		{
			name:      "missing input queue, refused as AccessDenied",
			overrides: map[string]string{"SQS_INPUT_QUEUE": "absent-queue.fifo"},
			want:      "absent-queue.fifo",
			failed:    "sqs",
			stopped:   []string{"postgres", "admin"},
		},
		{
			name:      "identity provider unreachable",
			overrides: map[string]string{"OIDC_JWKS_URL": "http://127.0.0.1:1/certs"},
			want:      "JWKS",
			failed:    "verifier",
			stopped:   []string{"sqs", "postgres", "admin"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := goleak.IgnoreCurrent()
			app := newApp(t, appConfig(t, tc.overrides))

			err := app.Start(t.Context())

			if err == nil {
				app.RequireStop()
				t.Fatal("Start() succeeded")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Start() error = %v, want it to name %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "s3cr3t") {
				t.Errorf("Start() error leaks the password: %v", err)
			}
			started := app.logs.components(t, "component started")
			stopped := app.logs.components(t, "component stopped")
			if !slices.Equal(stopped, tc.stopped) {
				t.Errorf("components stopped = %v, want %v", stopped, tc.stopped)
			}
			slices.Reverse(started)
			if !slices.Equal(stopped, started) {
				t.Errorf("components stopped = %v, want the started ones in reverse, %v", stopped, started)
			}
			var errorLines []string
			for _, r := range app.logs.records(t) {
				if r["level"] == slog.LevelError.String() {
					msg, _ := r["msg"].(string)
					cause, _ := r["error"].(string)
					name, _, _ := strings.Cut(cause, ": ")
					errorLines = append(errorLines, msg+" by "+name)
				}
			}
			if want := []string{"OnStart hook failed by " + tc.failed}; !slices.Equal(errorLines, want) {
				t.Errorf("error lines = %v, want the failure logged once, as %v", errorLines, want)
			}
			verifyNoLeaks(t, before)
		})
	}
}

var workerNames = []string{"consumer", "outbox-relay", "outbox-stats", "resolver"}

func stopOrder(logged []string) []string {
	order := slices.Clone(logged)
	isWorker := func(name string) bool { return slices.Contains(workerNames, name) }
	first := slices.IndexFunc(order, isWorker)
	if first < 0 {
		return order
	}
	end := first
	for end < len(order) && isWorker(order[end]) {
		end++
	}
	slices.Sort(order[first:end])
	return order
}

func TestApp_StopOrder(t *testing.T) {
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(collector.Close)
	tests := []struct {
		name      string
		overrides map[string]string
		start     []string
		stop      []string
	}{
		{
			name: "everything on",
			start: []string{"admin", "postgres", "sqs", "verifier", "http",
				"consumer", "outbox-relay", "outbox-stats", "resolver", "workers"},
			stop: []string{"drain", "consumer", "outbox-relay", "outbox-stats", "resolver", "workers",
				"http", "verifier", "sqs", "postgres", "admin"},
		},
		{
			name:      "tracing on",
			overrides: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": collector.URL},
			start: []string{"admin", "postgres", "sqs", "verifier", "http",
				"consumer", "outbox-relay", "outbox-stats", "resolver", "workers"},
			stop: []string{"drain", "consumer", "outbox-relay", "outbox-stats", "resolver", "workers",
				"http", "verifier", "sqs", "postgres", "admin", "tracing"},
		},
		{
			name:      "http off",
			overrides: map[string]string{"HTTP_ENABLED": "false", "SHUTDOWN_DRAIN_DELAY": "1h"},
			start: []string{"admin", "postgres", "sqs", "verifier",
				"consumer", "outbox-relay", "outbox-stats", "resolver", "workers"},
			stop: []string{"consumer", "outbox-relay", "outbox-stats", "resolver", "workers",
				"verifier", "sqs", "postgres", "admin"},
		},
		{
			name:      "consumer off",
			overrides: map[string]string{"CONSUMER_ENABLED": "false"},
			start: []string{"admin", "postgres", "sqs", "verifier", "http",
				"outbox-relay", "outbox-stats", "resolver", "workers"},
			stop: []string{"drain", "outbox-relay", "outbox-stats", "resolver", "workers",
				"http", "verifier", "sqs", "postgres", "admin"},
		},
		{
			name:      "outbox relay off",
			overrides: map[string]string{"OUTBOX_RELAY_ENABLED": "false"},
			start:     []string{"admin", "postgres", "sqs", "verifier", "http", "consumer", "resolver", "workers"},
			stop:      []string{"drain", "consumer", "resolver", "workers", "http", "verifier", "sqs", "postgres", "admin"},
		},
		{
			name:      "resolver off",
			overrides: map[string]string{"RESOLVER_ENABLED": "false"},
			start: []string{"admin", "postgres", "sqs", "verifier", "http",
				"consumer", "outbox-relay", "outbox-stats", "workers"},
			stop: []string{"drain", "consumer", "outbox-relay", "outbox-stats", "workers",
				"http", "verifier", "sqs", "postgres", "admin"},
		},
		{
			name: "every worker off",
			overrides: map[string]string{
				"CONSUMER_ENABLED": "false", "OUTBOX_RELAY_ENABLED": "false", "RESOLVER_ENABLED": "false",
			},
			start: []string{"admin", "postgres", "sqs", "verifier", "http"},
			stop:  []string{"drain", "http", "verifier", "sqs", "postgres", "admin"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := newApp(t, appConfig(t, tc.overrides))

			app.RequireStart().RequireStop()

			if got := app.logs.components(t, "component started"); !slices.Equal(got, tc.start) {
				t.Errorf("start order = %v, want %v", got, tc.start)
			}
			if got := stopOrder(app.logs.components(t, "component stopped")); !slices.Equal(got, tc.stop) {
				t.Errorf("stop order = %v, want %v", got, tc.stop)
			}
		})
	}
}

func TestApp_DrainKeepsServingWhileNotReady(t *testing.T) {
	app := newApp(t, appConfig(t, map[string]string{"SHUTDOWN_DRAIN_DELAY": "2s"}))
	app.RequireStart()
	api := newClient(t, "http://"+app.api.Addr())
	defer api.http.CloseIdleConnections()

	stopped := make(chan error, 1)
	go func() { stopped <- app.Stop(context.WithoutCancel(t.Context())) }()

	deadline := time.Now().Add(time.Second)
	for {
		code, _ := api.get("/health/ready")
		if code == http.StatusServiceUnavailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET /health/ready = %d a second into the stop, want 503", code)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if code, body := api.get("/health/live"); code != http.StatusOK {
		t.Fatalf("GET /health/live while draining = %d %s, want 200", code, body)
	}
	if err := <-stopped; err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}
