package metrics_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/goleak"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/metrics"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func observeAll(m *metrics.Metrics) {
	m.Transaction("http", "BET", "PROCESSED", "", 20*time.Millisecond)
	m.Duplicate("sqs", "inbox")
	m.TxRetry("serialization")
	m.ReconciliationDivergence()
	m.Message("processed")
	m.DLQ("INVALID_MESSAGE")
	m.Published("published")
	m.Pending(3, 90*time.Second)
	m.Resolution("processed")
	m.Observe("POST /wagering/transactions", http.MethodPost, http.StatusCreated, 15*time.Millisecond)
}

func TestMetrics_NamesRegistered(t *testing.T) {
	m, reg := metrics.New()
	observeAll(m)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := make(map[string]bool, len(families))
	for _, f := range families {
		got[f.GetName()] = true
	}

	for _, name := range []string{
		"wagering_transactions_total",
		"wagering_duplicates_total",
		"wagering_processing_seconds",
		"db_tx_retries_total",
		"sqs_messages_total",
		"sqs_dlq_total",
		"outbox_pending_events",
		"outbox_oldest_pending_age_seconds",
		"outbox_publish_total",
		"reference_resolutions_total",
		"reconciliation_divergences_total",
		"http_requests_total",
		"http_request_seconds",
	} {
		if !got[name] {
			t.Errorf("metric %s not registered", name)
		}
	}
}

func TestMetrics_Labels(t *testing.T) {
	m, reg := metrics.New()
	observeAll(m)
	m.Pending(7, 2*time.Minute)

	tests := []struct {
		metric string
		want   string
	}{
		{"wagering_transactions_total", `wagering_transactions_total{channel="http",failure_code="",kind="BET",status="PROCESSED"} 1`},
		{"wagering_processing_seconds", `wagering_processing_seconds_count{channel="http",kind="BET"} 1`},
		{"wagering_duplicates_total", `wagering_duplicates_total{channel="sqs",source="inbox"} 1`},
		{"db_tx_retries_total", `db_tx_retries_total{reason="serialization"} 1`},
		{"reconciliation_divergences_total", `reconciliation_divergences_total 1`},
		{"sqs_messages_total", `sqs_messages_total{result="processed"} 1`},
		{"sqs_dlq_total", `sqs_dlq_total{reason="INVALID_MESSAGE"} 1`},
		{"outbox_publish_total", `outbox_publish_total{result="published"} 1`},
		{"outbox_pending_events", `outbox_pending_events 7`},
		{"outbox_oldest_pending_age_seconds", `outbox_oldest_pending_age_seconds 120`},
		{"reference_resolutions_total", `reference_resolutions_total{result="processed"} 1`},
		{"http_requests_total", `http_requests_total{code="201",method="POST",route="POST /wagering/transactions"} 1`},
		{"http_request_seconds", `http_request_seconds_count{method="POST",route="POST /wagering/transactions"} 1`},
	}
	for _, tt := range tests {
		t.Run(tt.metric, func(t *testing.T) {
			if got := scrape(t, reg, tt.metric); !strings.Contains(got, tt.want+"\n") {
				t.Errorf("exposition of %s =\n%s\nwant a line %q", tt.metric, got, tt.want)
			}
		})
	}
}

func TestMetrics_SeparateRegistries(t *testing.T) {
	m1, reg1 := metrics.New()
	_, reg2 := metrics.New()

	m1.TxRetry("deadlock")

	if n := testutil.CollectAndCount(reg2, "db_tx_retries_total"); n != 0 {
		t.Errorf("second registry has %d db_tx_retries_total series, want 0", n)
	}
	if n := testutil.CollectAndCount(reg1, "db_tx_retries_total"); n != 1 {
		t.Errorf("first registry has %d db_tx_retries_total series, want 1", n)
	}
}

func TestAdmin_ServesMetrics(t *testing.T) {
	m, reg := metrics.New()
	m.Transaction("sqs", "WIN", "PROCESSED", "", time.Millisecond)
	srv := startAdmin(t, reg)

	code, body := get(t, "http://"+srv.Addr()+"/metrics")

	if code != http.StatusOK {
		t.Errorf("status = %d, want 200", code)
	}
	if !strings.Contains(body, "wagering_transactions_total") {
		t.Errorf("body does not contain wagering_transactions_total:\n%s", body)
	}
}

func TestAdmin_OnlyMetrics(t *testing.T) {
	_, reg := metrics.New()
	srv := startAdmin(t, reg)

	code, _ := get(t, "http://"+srv.Addr()+"/")

	if code != http.StatusNotFound {
		t.Errorf("status of / = %d, want 404", code)
	}
}

func TestAdmin_StartFailsWhenAddressInUse(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	_, reg := metrics.New()
	srv := metrics.NewAdminServer(ln.Addr().String(), reg)

	if err := srv.Start(t.Context()); err == nil {
		t.Error("Start succeeded on an address in use, want an error")
	}
}

func startAdmin(t *testing.T, reg *prometheus.Registry) *metrics.AdminServer {
	t.Helper()
	srv := metrics.NewAdminServer("127.0.0.1:0", reg)
	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Stop(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return srv
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(body)
}

func scrape(t *testing.T, reg *prometheus.Registry, metric string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	var b strings.Builder
	for line := range strings.Lines(rec.Body.String()) {
		if strings.HasPrefix(line, metric) {
			b.WriteString(line)
		}
	}
	return b.String()
}

func TestAdmin_StopIsIdempotent(t *testing.T) {
	_, reg := metrics.New()
	srv := metrics.NewAdminServer("127.0.0.1:0", reg)
	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for i := range 2 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		err := srv.Stop(ctx)
		cancel()
		if err != nil {
			t.Errorf("Stop #%d: %v", i+1, err)
		}
	}
}

func TestAdmin_StopBeforeStart(t *testing.T) {
	_, reg := metrics.New()
	srv := metrics.NewAdminServer("127.0.0.1:0", reg)

	if err := srv.Stop(t.Context()); err != nil {
		t.Errorf("Stop before Start: %v", err)
	}
}

func TestAdmin_StopPrefersFinishedServer(t *testing.T) {
	_, reg := metrics.New()
	srv := metrics.NewAdminServer("127.0.0.1:0", reg)
	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := srv.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	expired, cancel := context.WithCancel(t.Context())
	cancel()

	for i := range 50 {
		if err := srv.Stop(expired); err != nil {
			t.Fatalf("Stop #%d with an expired context after the server finished: %v", i+2, err)
		}
	}
}
