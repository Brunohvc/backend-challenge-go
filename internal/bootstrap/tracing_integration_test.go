//go:build integration

package bootstrap_test

import (
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/sqsin"
	"github.com/Brunohvc/backend-challenge-go/internal/bootstrap"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
)

const producerTraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func newTracedApp(t *testing.T) (*testApp, *tracetest.SpanRecorder) {
	t.Helper()
	tr, rec := tracingtest.New(t)
	cfg := appConfig(t, nil)
	a := &testApp{logs: &syncBuffer{}}
	a.App = fxtest.New(t,
		bootstrap.Module(cfg),
		fx.Replace(logging.New(a.logs, slog.LevelInfo, cfg.InstanceID)),
		fx.Replace(tr),
		fx.Populate(&a.admin, &a.api),
	)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("process log:\n%s", a.logs.buf.String())
		}
	})
	return a, rec
}

type spanTree struct {
	spans []sdktrace.ReadOnlySpan
}

func traceOf(rec *tracetest.SpanRecorder, id trace.TraceID) spanTree {
	var tree spanTree
	for _, s := range rec.Ended() {
		if s.SpanContext().TraceID() == id {
			tree.spans = append(tree.spans, s)
		}
	}
	return tree
}

func (tr spanTree) children(parent sdktrace.ReadOnlySpan, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range tr.spans {
		if s.Parent().SpanID() == parent.SpanContext().SpanID() && (name == "" || s.Name() == name) {
			out = append(out, s)
		}
	}
	return out
}

func (tr spanTree) child(t *testing.T, parent sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	found := tr.children(parent, name)
	if len(found) == 0 {
		t.Fatalf("no %q under %q; its children: %v", name, parent.Name(), names(tr.children(parent, "")))
	}
	return found[len(found)-1]
}

func (tr spanTree) requireNoValue(t *testing.T, values ...string) {
	t.Helper()
	for _, s := range tr.spans {
		for key, value := range tracingtest.Attributes(s) {
			for _, v := range values {
				if strings.Contains(value, v) {
					t.Errorf("span %q: %s = %q carries %q", s.Name(), key, value, v)
				}
			}
		}
	}
}

func names(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = s.Name()
	}
	return out
}

func requireStatements(t *testing.T, tree spanTree, unit sdktrace.ReadOnlySpan, want ...string) {
	t.Helper()
	got := names(tree.children(unit, ""))
	for _, name := range want {
		if !slices.Contains(got, name) {
			t.Errorf("statements under %s = %v, want %s among them", unit.Name(), got, name)
		}
	}
}

func eventuallySent(t *testing.T, rec *tracetest.SpanRecorder, id trace.TraceID, n int) spanTree {
	t.Helper()
	var tree spanTree
	eventually(t, "the relay to publish the events of the trace", func() bool {
		tree = traceOf(rec, id)
		return len(slices.DeleteFunc(slices.Clone(tree.spans), func(s sdktrace.ReadOnlySpan) bool {
			return s.Name() != "send wallet-events.fifo"
		})) >= n
	})
	return tree
}

func TestApp_TracesAnHTTPBetUpToItsPublishedEvents(t *testing.T) {
	internal := token(t, "wallet-service", "wallet-service-secret")
	provider := token(t, "provider-a", "provider-a-secret")
	app, rec := newTracedApp(t)
	app.RequireStart()
	defer app.RequireStop()
	api := newClient(t, "http://"+app.api.Addr())
	defer api.http.CloseIdleConnections()
	walletID, playerID := openWallet(t, api, internal)

	code, bet := api.call(http.MethodPost, "/wagering/transactions", provider, map[string]any{
		"providerId": "provider-a", "externalTransactionId": "bet-" + playerID, "playerId": playerID,
		"walletId": walletID, "roundId": "round-1", "gameId": "game-1", "kind": "BET",
		"money": map[string]string{"amount": "25.00", "currency": "BRL"},
	}, map[string]string{"Idempotency-Key": "key-" + playerID})
	if code != http.StatusCreated {
		t.Fatalf("POST /wagering/transactions = %d %v, want 201", code, bet)
	}

	server := tracingtest.One(t, rec, "POST /wagering/transactions")
	tree := eventuallySent(t, rec, server.SpanContext().TraceID(), 2)
	if server.SpanKind() != trace.SpanKindServer || server.Parent().IsValid() {
		t.Errorf("server span kind %v, parent valid %t; want the root of the trace", server.SpanKind(), server.Parent().IsValid())
	}
	submit := tree.child(t, server, "wagering.submit")
	attrs := tracingtest.Attributes(submit)
	if attrs["wagering.channel"] != "http" || attrs["wagering.status"] != "PROCESSED" || attrs["wallet.id"] != walletID ||
		attrs["wagering.transaction_id"] != bet["transactionId"] {
		t.Errorf("wagering.submit attributes = %v, want the processed bet of wallet %s", attrs, walletID)
	}
	tree.child(t, submit, "uow.snapshot")
	unit := tree.child(t, submit, "uow.do")
	requireStatements(t, tree, unit, "BEGIN", "SELECT wallets", "INSERT wager_transactions",
		"INSERT wallet_ledger_entries", "UPDATE wallets", "BATCH INSERT outbox_events", "COMMIT")
	if sends := tree.children(unit, "send wallet-events.fifo"); len(sends) != 2 {
		t.Errorf("%d sends under the unit of work that wrote the events, want 2", len(sends))
	}
	tree.requireNoValue(t, "25.00", "75.00", provider)
	var logged bool
	for _, line := range app.logs.records(t) {
		if line["msg"] == "http request" && line["route"] == "POST /wagering/transactions" {
			logged = line["traceId"] == server.SpanContext().TraceID().String()
		}
	}
	if !logged {
		t.Errorf("the access line of the bet does not carry its trace %s", server.SpanContext().TraceID())
	}
}

func TestApp_TracesAMessageFromTheProducersTrace(t *testing.T) {
	internal := token(t, "wallet-service", "wallet-service-secret")
	provider := token(t, "provider-a", "provider-a-secret")
	app, rec := newTracedApp(t)
	app.RequireStart()
	defer app.RequireStop()
	api := newClient(t, "http://"+app.api.Addr())
	defer api.http.CloseIdleConnections()
	walletID, playerID := openWallet(t, api, internal)

	_, err := sqsClient(t, "provider-a").SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(env.AWS.Queues.Input),
		MessageBody:            aws.String(wagerMessage(t, playerID, walletID, "BET")),
		MessageGroupId:         aws.String(walletID),
		MessageDeduplicationId: aws.String("message-" + playerID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			sqsin.ProviderTokenAttribute: {DataType: aws.String("String"), StringValue: aws.String(provider)},
			"traceparent":                {DataType: aws.String("String"), StringValue: aws.String(producerTraceParent)},
		},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	producer, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	tree := eventuallySent(t, rec, producer, 2)
	var process sdktrace.ReadOnlySpan
	for _, s := range tree.spans {
		if s.Name() == "process wager-transactions.fifo" {
			process = s
		}
	}
	if process == nil {
		t.Fatalf("no process span in the producer's trace: %v", names(tree.spans))
	}
	if process.Parent().SpanID().String() != "00f067aa0ba902b7" || !process.Parent().IsRemote() {
		t.Errorf("process span under %s, want the producer's span", process.Parent().SpanID())
	}
	if result := tracingtest.Attributes(process)["sqsin.result"]; result != "processed" {
		t.Errorf("sqsin.result = %q, want processed", result)
	}
	submit := tree.child(t, process, "wagering.submit")
	if channel := tracingtest.Attributes(submit)["wagering.channel"]; channel != "sqs" {
		t.Errorf("wagering.channel = %q, want sqs", channel)
	}
	unit := tree.child(t, submit, "uow.do")
	requireStatements(t, tree, unit, "INSERT inbox_messages", "UPDATE inbox_messages", "INSERT wager_transactions",
		"BATCH INSERT outbox_events", "COMMIT")
	tree.requireNoValue(t, "25.00", provider)
}
