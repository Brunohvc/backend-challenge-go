// Package logging builds the structured JSON logger and carries correlation fields in context.
package logging

import (
	"context"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// New returns a JSON logger that writes to w and drops records below level.
func New(w io.Writer, level slog.Level, instanceID string) *slog.Logger {
	inner := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: durationAsText})
	return slog.New(contextHandler{inner: inner}).With("instance", instanceID)
}

func durationAsText(_ []string, a slog.Attr) slog.Attr {
	if a.Value.Kind() == slog.KindDuration {
		return slog.String(a.Key, a.Value.Duration().String())
	}
	return a
}

type contextHandler struct {
	inner slog.Handler
}

func (h contextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	attrs := FieldsFrom(ctx).attrs()
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		attrs = append(attrs, slog.String("traceId", sc.TraceID().String()), slog.String("spanId", sc.SpanID().String()))
	}
	if len(attrs) > 0 {
		r = r.Clone()
		r.AddAttrs(attrs...)
	}
	return h.inner.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{inner: h.inner.WithGroup(name)}
}
