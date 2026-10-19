package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
)

func testHTTPConfig(addr string) config.HTTPConfig {
	return config.HTTPConfig{
		Addr:              addr,
		ReadHeaderTimeout: time.Second,
		ReadTimeout:       time.Second,
		WriteTimeout:      time.Second,
		IdleTimeout:       time.Second,
		ShutdownTimeout:   time.Second,
		RequestTimeout:    500 * time.Millisecond,
		MaxBodyBytes:      1024,
		MaxHeaderBytes:    4096,
	}
}

func startServer(t *testing.T) *Server {
	t.Helper()
	api := newTestAPI(t)
	srv := NewServer(testHTTPConfig("127.0.0.1:0"), api.handler, logging.New(io.Discard, 0, "test"))
	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return srv
}

func TestServer_ServesOnTheBoundPort(t *testing.T) {
	srv := startServer(t)
	t.Cleanup(func() { _ = srv.Stop(context.WithoutCancel(t.Context())) })

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+srv.Addr()+"/health/live", nil)
	if err != nil {
		t.Fatal(err)
	}
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestServer_StopIsIdempotent(t *testing.T) {
	srv := startServer(t)

	for i := range 2 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		err := srv.Stop(ctx)
		cancel()
		if err != nil {
			t.Errorf("Stop #%d: %v", i+1, err)
		}
	}
}

func TestServer_StopBeforeStart(t *testing.T) {
	srv := NewServer(testHTTPConfig("127.0.0.1:0"), http.NotFoundHandler(), logging.New(io.Discard, 0, "test"))

	if err := srv.Stop(t.Context()); err != nil {
		t.Errorf("Stop before Start: %v", err)
	}
	if srv.Addr() != "" {
		t.Errorf("Addr before Start = %q, want empty", srv.Addr())
	}
}

func TestServer_StartFailsWhenAddressInUse(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	srv := NewServer(testHTTPConfig(ln.Addr().String()), http.NotFoundHandler(), logging.New(io.Discard, 0, "test"))

	if err := srv.Start(t.Context()); err == nil {
		t.Error("Start succeeded on an address in use, want an error")
	}
}

func TestServer_AppliesConfiguredLimits(t *testing.T) {
	cfg := testHTTPConfig("127.0.0.1:0")
	srv := NewServer(cfg, http.NotFoundHandler(), logging.New(io.Discard, 0, "test"))

	s := srv.srv
	if s.ReadHeaderTimeout != cfg.ReadHeaderTimeout || s.ReadTimeout != cfg.ReadTimeout ||
		s.WriteTimeout != cfg.WriteTimeout || s.IdleTimeout != cfg.IdleTimeout || s.MaxHeaderBytes != cfg.MaxHeaderBytes {
		t.Errorf("server limits = %v %v %v %v %d, want the configured ones",
			s.ReadHeaderTimeout, s.ReadTimeout, s.WriteTimeout, s.IdleTimeout, s.MaxHeaderBytes)
	}
}

func TestServer_StopBoundedByContext(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	h := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	})
	cfg := testHTTPConfig("127.0.0.1:0")
	cfg.ShutdownTimeout = 10 * time.Second
	cfg.WriteTimeout = 10 * time.Second
	srv := NewServer(cfg, h, logging.New(io.Discard, 0, "test"))
	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	clientErr := make(chan error, 1)
	go func() {
		req, err := http.NewRequestWithContext(context.WithoutCancel(t.Context()), http.MethodGet, "http://"+srv.Addr()+"/slow", nil)
		if err != nil {
			clientErr <- err
			return
		}
		resp, err := (&http.Client{Transport: tr}).Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
		clientErr <- err
	}()
	<-entered

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := srv.Stop(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Stop returned nil with a request still in flight past its context")
	}
	if elapsed > time.Second {
		t.Errorf("Stop took %v, want it bounded by its 100ms context", elapsed)
	}
	select {
	case err := <-clientErr:
		if err == nil {
			t.Error("the in-flight request completed; want its connection cut")
		}
	case <-time.After(2 * time.Second):
		t.Error("the in-flight connection was not cut")
	}
	if err := srv.Stop(t.Context()); err != nil {
		t.Errorf("second Stop: %v, want nil once the server is closed", err)
	}
}

func TestServer_ReportsServingEndedOnItsOwn(t *testing.T) {
	srv := startServer(t)
	t.Cleanup(func() { _ = srv.Stop(context.WithoutCancel(t.Context())) })
	select {
	case <-srv.Done():
		t.Fatal("Done is closed while the server serves")
	default:
	}
	if err := srv.Err(); err != nil {
		t.Fatalf("Err() while serving = %v, want nil", err)
	}

	if err := srv.listener.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-srv.Done():
	case <-time.After(time.Second):
		t.Fatal("Done is still open a second after the listener failed")
	}
	if err := srv.Err(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("Err() = %v, want net.ErrClosed", err)
	}
	if err := srv.Stop(t.Context()); err != nil {
		t.Errorf("Stop() after serving ended = %v, want nil", err)
	}
}

func TestServer_StopEndsServingWithoutError(t *testing.T) {
	srv := startServer(t)

	if err := srv.Stop(t.Context()); err != nil {
		t.Fatalf("Stop() = %v", err)
	}

	select {
	case <-srv.Done():
	default:
		t.Fatal("Done is still open after Stop returned")
	}
	if err := srv.Err(); err != nil {
		t.Errorf("Err() after Stop = %v, want nil", err)
	}
}

func TestServer_StartsOnce(t *testing.T) {
	srv := startServer(t)
	t.Cleanup(func() { _ = srv.Stop(context.WithoutCancel(t.Context())) })

	if err := srv.Start(t.Context()); err == nil {
		t.Error("second Start succeeded, want an error")
	}
}

type hangingWagering struct {
	fakeWagering
}

func (*hangingWagering) Submit(ctx context.Context, _ wager.Command, _ wagering.Source) (wagering.Result, error) {
	<-ctx.Done()
	return wagering.Result{}, fmt.Errorf("%w: %w", app.ErrUnavailable, ctx.Err())
}

func TestServer_AnswersARequestWhoseWorkHangs(t *testing.T) {
	router := NewRouter(Deps{
		Verifier:     fakeVerifier{principals: wageringPrincipals},
		Health:       fakeHealth{ready: true},
		Metrics:      &fakeMetrics{},
		Log:          logging.New(io.Discard, 0, "test"),
		MaxBodyBytes: 65536,
		IDs:          fixedIDs{},
		Wallets:      &fakeWallets{},
		Wagering:     &hangingWagering{},
	})
	cfg := testHTTPConfig("127.0.0.1:0")
	cfg.RequestTimeout = 200 * time.Millisecond
	cfg.WriteTimeout = 5 * time.Second
	srv := NewServer(cfg, router, logging.New(io.Discard, 0, "test"))
	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.WithoutCancel(t.Context())) })

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+srv.Addr()+"/wagering/transactions", strings.NewReader(betBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+providerToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "provider-a:transaction-123")
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	began := time.Now()
	resp, err := (&http.Client{Transport: tr, Timeout: 2 * cfg.WriteTimeout}).Do(req)
	took := time.Since(began)
	if err != nil {
		t.Fatalf("POST got no answer after %s: %v", took, err)
	}
	defer func() { _ = resp.Body.Close() }()

	var p problem
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		t.Fatalf("decode the answer: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable || p.Code != codeTemporarilyUnavailable || resp.Header.Get("Retry-After") != retryAfterSeconds {
		t.Errorf("answer = %d %s with Retry-After %q, want 503 %s with Retry-After %s",
			resp.StatusCode, p.Code, resp.Header.Get("Retry-After"), codeTemporarilyUnavailable, retryAfterSeconds)
	}
	if took < cfg.RequestTimeout || took >= cfg.WriteTimeout {
		t.Errorf("answered after %s, want once the %s request timeout ended the work, well before the %s write timeout",
			took, cfg.RequestTimeout, cfg.WriteTimeout)
	}
}
