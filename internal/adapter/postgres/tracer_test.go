package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
)

func TestParseStatement_Summary(t *testing.T) {
	tests := []struct{ sql, want string }{
		{selectWallet + " FOR NO KEY UPDATE", "SELECT wallets"},
		{"SELECT id FROM wallets WHERE player_id = $1", "SELECT wallets"},
		{"select id\n\t from wager_transactions where id = $1", "SELECT wager_transactions"},
		{"INSERT INTO wallet_ledger_entries (id) VALUES ($1)", "INSERT wallet_ledger_entries"},
		{appendOutboxEvent, "INSERT outbox_events"},
		{"UPDATE wallets\n SET balance_minor = $2 WHERE id = $1", "UPDATE wallets"},
		{"DELETE FROM inbox_messages WHERE consumer_name = $1", "DELETE inbox_messages"},
		{claimOutboxHeads, "WITH outbox_events"},
		{claimDue, "WITH wager_transactions"},
		{"begin isolation level read committed read write", "BEGIN"},
		{"commit", "COMMIT"},
		{"rollback", "ROLLBACK"},
		{"SELECT version, dirty FROM public.schema_version()", "SELECT"},
		{"SELECT count(*), 1 FROM (SELECT 1) AS x", "SELECT"},
		{`SELECT 'ignore everything' FROM "Quoted Table" WHERE 1=1`, "SELECT"},
		{"VACUUM wallets", "QUERY"},
		{"", "QUERY"},
	}
	for _, tt := range tests {
		if got := parseStatement(tt.sql).summary(); got != tt.want {
			t.Errorf("parseStatement(%q).summary() = %q, want %q", tt.sql, got, tt.want)
		}
	}
}

func TestTracePool_SetsTheTracer(t *testing.T) {
	tr, _ := tracingtest.New(t)
	cfg := config.DBConfig{URL: "postgres://u:p@localhost:5432/db", MaxConns: 1}
	tests := []struct {
		name   string
		opts   []PoolOption
		traced bool
	}{
		{"without tracing", nil, false},
		{"with a tracer that is off", []PoolOption{WithTracing(tracing.Tracer{})}, false},
		{"with a tracer that is on", []PoolOption{WithTracing(tr)}, true},
	}
	for _, tt := range tests {
		pc, err := poolConfig(cfg, "test", tt.opts...)
		if err != nil {
			t.Fatalf("poolConfig() %s error = %v", tt.name, err)
		}
		if _, ok := pc.ConnConfig.Tracer.(pgx.BatchTracer); ok != tt.traced {
			t.Errorf("Tracer = %T %s, want a query and batch tracer = %t", pc.ConnConfig.Tracer, tt.name, tt.traced)
		}
	}
}

func TestQueryTracer_OnlyUnderARecordingSpan(t *testing.T) {
	tr, rec := tracingtest.New(t)
	qt := queryTracer{tracer: tr}

	ctx := qt.TraceQueryStart(t.Context(), nil, pgx.TraceQueryStartData{SQL: "SELECT id FROM wallets WHERE id = $1"})
	qt.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})

	if trace.SpanFromContext(ctx).SpanContext().IsValid() {
		t.Error("TraceQueryStart() started a span without a parent")
	}
	if names := tracingtest.Names(rec); len(names) != 0 {
		t.Errorf("ended spans = %v, want none", names)
	}
}

func TestQueryTracer_RecordsTheStatementNotItsValues(t *testing.T) {
	tr, rec := tracingtest.New(t)
	ctx, root := tracingtest.Root(t.Context(), tr)
	qt := queryTracer{tracer: tr}
	const sql = "UPDATE wallets SET balance_minor = $2 WHERE id = $1"

	qctx := qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: sql, Args: []any{"0192f291-27dd", int64(2500)}})
	qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{CommandTag: pgconn.NewCommandTag("UPDATE 1")})
	root.End()

	span := tracingtest.One(t, rec, "UPDATE wallets")
	if !tracingtest.ChildOf(span, tracingtest.One(t, rec, "test")) || span.SpanKind() != trace.SpanKindClient {
		t.Errorf("span kind %v, child of the root = %t; want a client span under it", span.SpanKind(),
			tracingtest.ChildOf(span, tracingtest.One(t, rec, "test")))
	}
	want := map[string]string{
		"db.system.name":     "postgresql",
		"db.operation.name":  "UPDATE",
		"db.collection.name": "wallets",
		"db.query.summary":   "UPDATE wallets",
		"db.query.text":      sql,
	}
	attrs := tracingtest.Attributes(span)
	for key, value := range want {
		if attrs[key] != value {
			t.Errorf("%s = %q, want %q", key, attrs[key], value)
		}
	}
	for key, value := range attrs {
		if strings.Contains(value, "2500") || strings.Contains(value, "0192f291") {
			t.Errorf("%s = %q carries an argument of the statement", key, value)
		}
	}
	if span.Status().Code == codes.Error {
		t.Errorf("Status() = %+v, want no error", span.Status())
	}
}

func TestQueryTracer_RecordsTheFailure(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantType string
	}{
		{"a server error by its SQLSTATE", &pgconn.PgError{Code: "40P01", Message: "deadlock with 2500"}, "40P01"},
		{"a dropped call by its class", context.Canceled, "canceled"},
		{"anything else as permanent", errors.New("conn busy"), "permanent"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, rec := tracingtest.New(t)
			ctx, root := tracingtest.Root(t.Context(), tr)
			qt := queryTracer{tracer: tr}

			qctx := qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT id FROM wallets"})
			qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{Err: tt.err})
			root.End()

			span := tracingtest.One(t, rec, "SELECT wallets")
			attrs := tracingtest.Attributes(span)
			if span.Status().Code != codes.Error || attrs["error.type"] != tt.wantType {
				t.Errorf("status %+v, error.type %q; want an error of type %q", span.Status(), attrs["error.type"], tt.wantType)
			}
			if strings.Contains(span.Status().Description, "2500") {
				t.Errorf("status %q carries the server message", span.Status().Description)
			}
		})
	}
}

func TestQueryTracer_BatchIsOneSpan(t *testing.T) {
	tests := []struct {
		name  string
		sqls  []string
		want  string
		attrs map[string]string
	}{
		{
			name: "the same statement",
			sqls: []string{appendOutboxEvent, appendOutboxEvent},
			want: "BATCH INSERT outbox_events",
			attrs: map[string]string{
				"db.operation.batch.size": "2", "db.operation.name": "BATCH INSERT", "db.collection.name": "outbox_events",
				"db.query.text": appendOutboxEvent,
			},
		},
		{
			name:  "different statements",
			sqls:  []string{appendOutboxEvent, "UPDATE wallets SET version = 2"},
			want:  "BATCH",
			attrs: map[string]string{"db.operation.batch.size": "2", "db.operation.name": "BATCH"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, rec := tracingtest.New(t)
			ctx, root := tracingtest.Root(t.Context(), tr)
			qt := queryTracer{tracer: tr}
			batch := &pgx.Batch{}
			for _, sql := range tt.sqls {
				batch.Queue(sql, "a value that must not leak")
			}

			bctx := qt.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{Batch: batch})
			for _, sql := range tt.sqls {
				qt.TraceBatchQuery(bctx, nil, pgx.TraceBatchQueryData{SQL: sql})
			}
			qt.TraceBatchEnd(bctx, nil, pgx.TraceBatchEndData{})
			root.End()

			span := tracingtest.One(t, rec, tt.want)
			attrs := tracingtest.Attributes(span)
			for key, value := range tt.attrs {
				if attrs[key] != value {
					t.Errorf("%s = %q, want %q", key, attrs[key], value)
				}
			}
			for key, value := range attrs {
				if strings.Contains(value, "must not leak") {
					t.Errorf("%s = %q carries an argument", key, value)
				}
			}
		})
	}
}

func TestQueryTracer_BatchRecordsItsFailure(t *testing.T) {
	tr, rec := tracingtest.New(t)
	ctx, root := tracingtest.Root(t.Context(), tr)
	qt := queryTracer{tracer: tr}
	batch := &pgx.Batch{}
	batch.Queue(appendOutboxEvent)
	batch.Queue(appendOutboxEvent)
	failure := &pgconn.PgError{Code: "23505", Message: "duplicate key 2500"}

	bctx := qt.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{Batch: batch})
	qt.TraceBatchQuery(bctx, nil, pgx.TraceBatchQueryData{SQL: appendOutboxEvent, Err: failure})
	qt.TraceBatchEnd(bctx, nil, pgx.TraceBatchEndData{Err: failure})
	root.End()

	span := tracingtest.One(t, rec, "BATCH INSERT outbox_events")
	attrs := tracingtest.Attributes(span)
	if span.Status().Code != codes.Error || attrs["error.type"] != "23505" || attrs["db.response.status_code"] != "23505" {
		t.Errorf("status %+v, error.type %q, status code %q; want the failure 23505", span.Status(),
			attrs["error.type"], attrs["db.response.status_code"])
	}
}

func TestQueryTracer_ABatchWithoutQueriesEndsOnlyItsOwnSpan(t *testing.T) {
	tr, rec := tracingtest.New(t)
	ctx, root := tracingtest.Root(t.Context(), tr)
	qt := queryTracer{tracer: tr}

	bctx := qt.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{})
	qt.TraceBatchEnd(bctx, nil, pgx.TraceBatchEndData{})

	if !root.IsRecording() {
		t.Fatal("TraceBatchEnd() ended the span of the caller")
	}
	root.End()
	span := tracingtest.One(t, rec, "BATCH")
	if !tracingtest.ChildOf(span, tracingtest.One(t, rec, "test")) {
		t.Error("BATCH is not under the span of the caller")
	}
	if size := tracingtest.Attributes(span)["db.operation.batch.size"]; size != "0" {
		t.Errorf("db.operation.batch.size = %q, want 0", size)
	}
}

func TestQueryTracer_OffLeavesTheCallersSpanOpen(t *testing.T) {
	on, rec := tracingtest.New(t)
	ctx, root := tracingtest.Root(t.Context(), on)
	qt := queryTracer{tracer: tracing.Tracer{}}

	qctx := qt.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT id FROM wallets"})
	qt.TraceQueryEnd(qctx, nil, pgx.TraceQueryEndData{Err: &pgconn.PgError{Code: "40P01"}})
	bctx := qt.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{})
	qt.TraceBatchEnd(bctx, nil, pgx.TraceBatchEndData{Err: errors.New("conn busy")})

	if !root.IsRecording() {
		t.Fatal("a tracer that is off ended the span of the caller")
	}
	root.End()
	if names := tracingtest.Names(rec); len(names) != 1 {
		t.Errorf("ended spans = %v, want only the caller's", names)
	}
	span := tracingtest.One(t, rec, "test")
	if attrs := tracingtest.Attributes(span); len(attrs) != 0 || span.Status().Code != codes.Unset {
		t.Errorf("the caller's span got attributes %v and status %+v, want neither", attrs, span.Status())
	}
}
