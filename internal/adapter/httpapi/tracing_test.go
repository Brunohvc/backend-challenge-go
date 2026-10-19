package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
)

const callerTraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

type spanWagering struct {
	*fakeWagering
	seen []trace.SpanContext
}

func (s *spanWagering) Submit(ctx context.Context, cmd wager.Command, src wagering.Source) (wagering.Result, error) {
	s.seen = append(s.seen, trace.SpanContextFromContext(ctx))
	return s.fakeWagering.Submit(ctx, cmd, src)
}

func newTracedAPI(t *testing.T, tr tracing.Tracer) (*wageringAPI, *spanWagering) {
	t.Helper()
	logs := &bytes.Buffer{}
	svc := &spanWagering{fakeWagering: &fakeWagering{result: wagering.Result{
		Transaction: snapshot(wager.StatusProcessed, "", mustMoney("975.00")),
	}}}
	h := NewRouter(Deps{
		Verifier:     fakeVerifier{principals: wageringPrincipals},
		Health:       fakeHealth{ready: true},
		Metrics:      &fakeMetrics{},
		Log:          logging.New(logs, -4, "test"),
		MaxBodyBytes: 65536,
		IDs:          fixedIDs{},
		Wallets:      &fakeWallets{},
		Wagering:     svc,
		Tracer:       tr,
	})
	return &wageringAPI{handler: h, svc: svc.fakeWagering, logs: logs}, svc
}

func accessLine(t *testing.T, logs *bytes.Buffer) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(logs.Bytes()))
	for dec.More() {
		var line map[string]any
		if err := dec.Decode(&line); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		if line["msg"] == "http request" {
			return line
		}
	}
	t.Fatalf("no access line in %s", logs)
	return nil
}

func TestTracing_NamesTheServerSpanAfterTheRoute(t *testing.T) {
	tr, rec := tracingtest.New(t)
	api, svc := newTracedAPI(t, tr)

	if got := api.submit(t, betBody); got.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %s)", got.Code, got.Body)
	}

	span := tracingtest.One(t, rec, "POST /wagering/transactions")
	if span.SpanKind() != trace.SpanKindServer || span.Parent().IsValid() {
		t.Errorf("span kind %v, parent valid %t; want a server span that starts the trace", span.SpanKind(), span.Parent().IsValid())
	}
	attrs := tracingtest.Attributes(span)
	want := map[string]string{
		"http.route":                "/wagering/transactions",
		"http.request.method":       "POST",
		"http.response.status_code": "201",
	}
	for key, value := range want {
		if attrs[key] != value {
			t.Errorf("%s = %q, want %q", key, attrs[key], value)
		}
	}
	for key, value := range attrs {
		if strings.Contains(value, "25.00") || strings.Contains(value, providerToken) {
			t.Errorf("%s = %q carries the amount or the token", key, value)
		}
	}
	if len(svc.seen) != 1 || !svc.seen[0].Equal(span.SpanContext()) {
		t.Errorf("the use case ran under %v, want the server span %s", svc.seen, span.SpanContext().SpanID())
	}
	line := accessLine(t, api.logs)
	if line["traceId"] != span.SpanContext().TraceID().String() || line["spanId"] != span.SpanContext().SpanID().String() {
		t.Errorf("access line traceId %v spanId %v, want the server span", line["traceId"], line["spanId"])
	}
}

func TestTracing_SpanNameKeepsThePathParametersOut(t *testing.T) {
	tr, rec := tracingtest.New(t)
	api, _ := newTracedAPI(t, tr)

	api.do(t, http.MethodGet, "/wagering/transactions/"+exampleTx.String(), "", providerToken, nil)

	span := tracingtest.One(t, rec, "GET /wagering/transactions/{transactionId}")
	if attrs := tracingtest.Attributes(span); attrs["http.route"] != "/wagering/transactions/{transactionId}" {
		t.Errorf("http.route = %q, want the pattern", attrs["http.route"])
	}
}

func TestTracing_ContinuesTheCallersTrace(t *testing.T) {
	tr, rec := tracingtest.New(t)
	api, _ := newTracedAPI(t, tr)
	req := request(t, http.MethodPost, "/wagering/transactions", betBody, providerToken)
	req.Header.Set("Idempotency-Key", "provider-a:transaction-123")
	req.Header.Set("traceparent", callerTraceParent)

	api.send(req)

	span := tracingtest.One(t, rec, "POST /wagering/transactions")
	if span.SpanContext().TraceID().String() != "0af7651916cd43dd8448eb211c80319c" ||
		span.Parent().SpanID().String() != "b7ad6b7169203331" {
		t.Errorf("trace %s under %s, want the caller's", span.SpanContext().TraceID(), span.Parent().SpanID())
	}
}

func TestTracing_UnmatchedKeepsOnlyTheMethod(t *testing.T) {
	tr, rec := tracingtest.New(t)
	api, _ := newTracedAPI(t, tr)

	api.do(t, http.MethodGet, "/wagering/everything/"+exampleTx.String(), "", providerToken, nil)

	span := tracingtest.One(t, rec, "GET")
	if attrs := tracingtest.Attributes(span); attrs["http.route"] != "" || attrs["http.response.status_code"] != "404" {
		t.Errorf("http.route = %q, status %q; want no route and 404", attrs["http.route"], attrs["http.response.status_code"])
	}
}

func TestTracing_HealthIsNotTraced(t *testing.T) {
	tr, rec := tracingtest.New(t)
	api, _ := newTracedAPI(t, tr)

	for _, path := range []string{"/health/live", "/health/ready"} {
		if got := api.do(t, http.MethodGet, path, "", "", nil); got.Code != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", path, got.Code)
		}
	}

	if names := tracingtest.Names(rec); len(names) != 0 {
		t.Errorf("ended spans = %v, want none for health checks", names)
	}
}

func TestTracing_OffLeavesTheRequestUntraced(t *testing.T) {
	api, svc := newTracedAPI(t, tracing.Tracer{})
	req := request(t, http.MethodPost, "/wagering/transactions", betBody, providerToken)
	req.Header.Set("Idempotency-Key", "provider-a:transaction-123")
	req.Header.Set("traceparent", callerTraceParent)

	if got := api.send(req); got.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", got.Code)
	}

	if len(svc.seen) != 1 || svc.seen[0].IsValid() {
		t.Errorf("the use case ran under %v, want no span context with tracing off", svc.seen)
	}
	if line := accessLine(t, api.logs); line["traceId"] != nil {
		t.Errorf("access line traceId = %v, want none", line["traceId"])
	}
}
