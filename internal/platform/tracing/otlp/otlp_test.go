package otlp_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/otlp"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const (
	sampledParent   = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	unsampledParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type collector struct {
	mu       sync.Mutex
	requests []exported
}

type exported struct {
	path, contentType string
	body              []byte
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	c.requests = append(c.requests, exported{path: r.URL.Path, contentType: r.Header.Get("Content-Type"), body: body})
	c.mu.Unlock()
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
}

func (c *collector) received() []exported {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]exported(nil), c.requests...)
}

func newCollector(t *testing.T) (*collector, string) {
	t.Helper()
	c := &collector{}
	srv := httptest.NewServer(c)
	t.Cleanup(srv.Close)
	return c, srv.URL
}

func enabledProvider(t *testing.T, endpoint, instanceID string) *otlp.Provider {
	t.Helper()
	p, err := otlp.New(config.TracingConfig{Endpoint: endpoint, ServiceName: "wallet"}, instanceID, discardLog())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func TestNew_WithoutEndpointStartsNothing(t *testing.T) {
	p, err := otlp.New(config.TracingConfig{ServiceName: "wallet"}, "app-1", discardLog())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	goleak.VerifyNone(t)

	tr := p.Tracer()
	if tr.Enabled() {
		t.Error("Enabled() = true, want false")
	}
	if tr.Provider() != nil {
		t.Errorf("Provider() = %v, want nil", tr.Provider())
	}
	ctx := tr.Extract(context.Background(), propagation.MapCarrier{"traceparent": sampledParent})
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("Extract() read a span context while tracing is off")
	}
	ctx, span := tr.Start(ctx, "request")
	if span.IsRecording() || span.SpanContext().IsValid() {
		t.Errorf("Start() span recording = %t, valid = %t, want neither", span.IsRecording(), span.SpanContext().IsValid())
	}
	if _, child := tr.Child(ctx, "unit of work"); child.IsRecording() {
		t.Error("Child() recorded a span while tracing is off")
	}
	if err := p.Shutdown(t.Context()); err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}
}

func TestNew_ExportsTheQueuedSpansOnShutdown(t *testing.T) {
	c, endpoint := newCollector(t)
	p := enabledProvider(t, endpoint, "app-7")

	_, span := p.Tracer().Start(t.Context(), "probe")
	span.End()
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	got := c.received()
	if len(got) != 1 {
		t.Fatalf("collector received %d requests, want 1", len(got))
	}
	if got[0].path != "/v1/traces" || got[0].contentType != "application/x-protobuf" {
		t.Errorf("request = %s (%s), want /v1/traces (application/x-protobuf)", got[0].path, got[0].contentType)
	}
	for _, want := range []string{"probe", "service.name", "wallet", "service.instance.id", "app-7"} {
		if !bytes.Contains(got[0].body, []byte(want)) {
			t.Errorf("exported spans do not carry %q", want)
		}
	}
}

func TestNew_SamplesNewTracesAndFollowsTheCaller(t *testing.T) {
	_, endpoint := newCollector(t)
	tr := enabledProvider(t, endpoint, "app-1").Tracer()

	tests := []struct {
		name        string
		traceparent string
		want        bool
	}{
		{"a new trace", "", true},
		{"a caller that samples", sampledParent, true},
		{"a caller that does not", unsampledParent, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tr.Extract(t.Context(), propagation.MapCarrier{"traceparent": tt.traceparent})
			_, span := tr.Start(ctx, "request")
			defer span.End()
			if span.IsRecording() != tt.want {
				t.Errorf("IsRecording() = %t, want %t", span.IsRecording(), tt.want)
			}
		})
	}
}
