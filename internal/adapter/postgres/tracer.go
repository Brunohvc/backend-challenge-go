package postgres

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

const (
	summaryUnknown = "QUERY"
	batchPrefix    = "BATCH"
)

var operations = map[string]bool{
	"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "WITH": true,
	"BEGIN": true, "COMMIT": true, "ROLLBACK": true,
}

var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// PoolOption adjusts the pool NewPool opens.
type PoolOption func(*pgxpool.Config)

// WithTracing records each statement as a span on tr under the recording span of its context, unless tr is off.
func WithTracing(tr tracing.Tracer) PoolOption {
	return func(pc *pgxpool.Config) {
		if tr.Enabled() {
			pc.ConnConfig.Tracer = queryTracer{tracer: tr}
		}
	}
}

type queryTracer struct {
	tracer tracing.Tracer
}

type statementSpanKey struct{}

var (
	_ pgx.QueryTracer = queryTracer{}
	_ pgx.BatchTracer = queryTracer{}
)

type statement struct {
	operation, collection string
}

func (s statement) summary() string {
	if s.collection == "" {
		return s.operation
	}
	return s.operation + " " + s.collection
}

func (s statement) attributes() []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.DBSystemNamePostgreSQL,
		semconv.DBOperationName(s.operation),
		semconv.DBQuerySummary(s.summary()),
	}
	if s.collection != "" {
		attrs = append(attrs, semconv.DBCollectionName(s.collection))
	}
	return attrs
}

func parseStatement(sql string) statement {
	words := strings.Fields(sql)
	if len(words) == 0 || !operations[strings.ToUpper(words[0])] {
		return statement{operation: summaryUnknown}
	}
	s := statement{operation: strings.ToUpper(words[0])}
	after := "FROM"
	switch s.operation {
	case "INSERT":
		after = "INTO"
	case "UPDATE":
		after = "UPDATE"
	}
	for i, w := range words[:len(words)-1] {
		if strings.EqualFold(w, after) {
			if next := words[i+1]; identifier.MatchString(next) {
				s.collection = next
			}
			break
		}
	}
	return s
}

func (qt queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}
	s := parseStatement(data.SQL)
	return qt.startStatement(ctx, s.summary(), append(s.attributes(), semconv.DBQueryText(data.SQL)))
}

func (queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	endStatement(ctx, data.Err)
}

func (qt queryTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}
	var queued []*pgx.QueuedQuery
	if data.Batch != nil {
		queued = data.Batch.QueuedQueries
	}
	s := statement{operation: batchPrefix}
	attrs := []attribute.KeyValue{semconv.DBOperationBatchSize(len(queued))}
	if each, ok := sameStatement(queued); ok {
		s = statement{operation: batchPrefix + " " + each.operation, collection: each.collection}
		attrs = append(attrs, semconv.DBQueryText(queued[0].SQL))
	}
	return qt.startStatement(ctx, s.summary(), append(s.attributes(), attrs...))
}

func sameStatement(queued []*pgx.QueuedQuery) (statement, bool) {
	if len(queued) == 0 {
		return statement{}, false
	}
	for _, q := range queued[1:] {
		if q.SQL != queued[0].SQL {
			return statement{}, false
		}
	}
	return parseStatement(queued[0].SQL), true
}

func (queryTracer) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

func (queryTracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	endStatement(ctx, data.Err)
}

func (qt queryTracer) startStatement(ctx context.Context, name string, attrs []attribute.KeyValue) context.Context {
	ctx, span := qt.tracer.Child(ctx, name, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	return context.WithValue(ctx, statementSpanKey{}, span)
}

func endStatement(ctx context.Context, err error) {
	span, ok := ctx.Value(statementSpanKey{}).(trace.Span)
	if !ok || !span.IsRecording() {
		return
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			span.SetAttributes(semconv.DBResponseStatusCode(pgErr.Code))
			tracing.Fail(span, pgErr.Code)
		} else {
			tracing.Fail(span, app.ErrorType(classify(err)))
		}
	}
	span.End()
}
