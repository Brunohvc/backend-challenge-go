package bootstrap

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/fx/fxtest"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/otlp"
)

func tracingLifecycle(t *testing.T, cfg config.Config, out *bytes.Buffer) (*fxtest.Lifecycle, *otlp.Provider) {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(out, nil))
	p, err := newTracing(cfg, log)
	if err != nil {
		t.Fatalf("newTracing() error = %v", err)
	}
	lc := fxtest.NewLifecycle(t)
	registerTracing(lifecycle{Lifecycle: lc, Running: newRunningHook(), Log: log}, cfg, p)
	return lc, p
}

func tracingConfig(endpoint string) config.Config {
	return config.Config{InstanceID: "app-1", Tracing: config.TracingConfig{Endpoint: endpoint, ServiceName: "wallet"}}
}

func TestTracing_OffAddsNoComponent(t *testing.T) {
	var out bytes.Buffer
	lc, p := tracingLifecycle(t, tracingConfig(""), &out)

	lc.RequireStart().RequireStop()

	if p.Tracer().Enabled() {
		t.Error("the tracer is on without an endpoint")
	}
	if out.Len() != 0 {
		t.Errorf("logged %q, want nothing", out.String())
	}
}

func TestTracing_StopExportsTheQueuedSpans(t *testing.T) {
	var exports atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		exports.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	var out bytes.Buffer
	lc, p := tracingLifecycle(t, tracingConfig(srv.URL), &out)
	lc.RequireStart()
	_, span := p.Tracer().Start(t.Context(), "request")
	span.End()

	lc.RequireStop()

	if exports.Load() != 1 {
		t.Errorf("collector received %d exports, want the queued span exported on stop", exports.Load())
	}
	recs := records(t, &out)
	if len(recs) != 1 || recs[0]["msg"] != "component stopped" || recs[0]["component"] != tracingName {
		t.Errorf("logged %v, want only the tracing component stopped", recs)
	}
}

func TestTracing_AFlushCutShortOnlyWarns(t *testing.T) {
	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-unblock:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(unblock) })
	var out bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&out, nil))
	p, err := newTracing(tracingConfig(srv.URL), log)
	if err != nil {
		t.Fatalf("newTracing() error = %v", err)
	}
	_, span := p.Tracer().Start(t.Context(), "request")
	span.End()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	if err := flushSpans(p, log)(ctx); err != nil {
		t.Fatalf("flush error = %v, want nil: losing spans must not fail the shutdown", err)
	}

	recs := records(t, &out)
	if len(recs) != 1 || recs[0]["level"] != "WARN" || recs[0]["msg"] != "spans still queued were not exported" {
		t.Errorf("logged %v, want one warning about the spans left", recs)
	}
}
