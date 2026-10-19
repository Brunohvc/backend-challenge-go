package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
)

const headerCorrelationID = "X-Correlation-Id"

const (
	routeUnmatched = "unmatched"
	methodOther    = "OTHER"
)

var standardMethods = map[string]bool{
	http.MethodGet: true, http.MethodPost: true, http.MethodPut: true, http.MethodPatch: true,
	http.MethodDelete: true, http.MethodHead: true, http.MethodOptions: true,
}

func (h *handlers) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := &statusWriter{ResponseWriter: w}
		defer h.answerPanic(rw, r)
		next.ServeHTTP(rw, r)
	})
}

func (h *handlers) answerPanic(rw *statusWriter, r *http.Request) {
	v := recover()
	if v == nil {
		return
	}
	if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
		panic(v)
	}
	h.deps.Log.ErrorContext(r.Context(), "panic serving request",
		slog.String("correlationId", rw.Header().Get(headerCorrelationID)),
		slog.String("panic", fmt.Sprint(v)),
		slog.String("stack", string(debug.Stack())))
	if !rw.wroteHeader() {
		writeProblem(rw, r, http.StatusInternalServerError, codeInternalError, "Internal error", "", nil)
	}
}

func (h *handlers) correlate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(headerCorrelationID)
		if !wager.ValidTextID(id) {
			id = h.deps.IDs.New().String()
		}
		w.Header().Set(headerCorrelationID, id)
		next.ServeHTTP(w, r.WithContext(logging.With(r.Context(), logging.Fields{CorrelationID: id})))
	})
}

type requestInfo struct {
	providerID string
}

type requestInfoKey struct{}

func infoFrom(ctx context.Context) *requestInfo {
	info, _ := ctx.Value(requestInfoKey{}).(*requestInfo)
	return info
}

func (h *handlers) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		info := &requestInfo{}
		r = r.WithContext(context.WithValue(r.Context(), requestInfoKey{}, info))
		rw := &statusWriter{ResponseWriter: w}
		completed := false
		defer h.record(r, rw, info, start, &completed)
		next.ServeHTTP(rw, r)
		completed = true
	})
}

func (h *handlers) record(r *http.Request, rw *statusWriter, info *requestInfo, start time.Time, completed *bool) {
	status := rw.status
	switch {
	case status == 0 && *completed:
		status = http.StatusOK
	case status == 0:
		status = http.StatusInternalServerError
	}
	d := time.Since(start)
	route := r.Pattern
	if route == "" {
		route = routeUnmatched
	} else {
		nameSpan(r)
	}
	method := r.Method
	if !standardMethods[method] {
		method = methodOther
	}
	h.deps.Metrics.Observe(route, method, status, d)
	attrs := []slog.Attr{
		slog.String("method", method),
		slog.String("route", route),
		slog.Int("status", status),
		slog.Int64("durationMs", d.Milliseconds()),
	}
	if info.providerID != "" {
		attrs = append(attrs, slog.String("providerId", info.providerID))
	}
	h.deps.Log.LogAttrs(r.Context(), slog.LevelInfo, "http request", attrs...)
}

func nameSpan(r *http.Request) {
	span := trace.SpanFromContext(r.Context())
	if !span.IsRecording() {
		return
	}
	span.SetName(r.Pattern)
	if _, path, ok := strings.Cut(r.Pattern, " "); ok {
		span.SetAttributes(semconv.HTTPRoute(path))
	}
}

func (h *handlers) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			h.logAuthFailure(r, slog.LevelDebug, "missing")
			unauthenticated(w, r, `Bearer realm="wagering"`)
			return
		}
		p, err := h.deps.Verifier.Verify(r.Context(), raw)
		if reason, level, ok := authFailure(err); ok {
			h.logAuthFailure(r, level, reason)
			unauthenticated(w, r, `Bearer realm="wagering", error="invalid_token"`)
			return
		}
		if err != nil {
			h.deps.Log.ErrorContext(r.Context(), "verify access token", slog.String("error", err.Error()))
			writeProblem(w, r, http.StatusServiceUnavailable, codeTemporarilyUnavailable,
				"Temporarily unavailable", "the access token could not be verified now", nil)
			return
		}
		if info := infoFrom(r.Context()); info != nil {
			info.providerID = p.ProviderID
		}
		ctx := oidc.WithPrincipal(r.Context(), p)
		ctx = logging.With(ctx, logging.Fields{ProviderID: p.ProviderID})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func authFailure(err error) (reason string, level slog.Level, ok bool) {
	switch {
	case errors.Is(err, oidc.ErrMisconfiguredPrincipal):
		return "misconfigured", slog.LevelWarn, true
	case errors.Is(err, oidc.ErrExpiredToken):
		return "expired", slog.LevelDebug, true
	case errors.Is(err, oidc.ErrMissingToken):
		return "missing", slog.LevelDebug, true
	case errors.Is(err, oidc.ErrInvalidToken):
		return "invalid", slog.LevelDebug, true
	}
	return "", 0, false
}

func (h *handlers) logAuthFailure(r *http.Request, level slog.Level, reason string) {
	h.deps.Log.LogAttrs(r.Context(), level, "authentication failed", slog.String("reason", reason))
}

func bearerToken(header string) (string, bool) {
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != ""
}

func unauthenticated(w http.ResponseWriter, r *http.Request, challenge string) {
	w.Header().Set("WWW-Authenticate", challenge)
	writeProblem(w, r, http.StatusUnauthorized, codeUnauthenticated, "Unauthenticated",
		"a valid bearer access token is required", nil)
}

func (h *handlers) requireInternal(next http.Handler) http.Handler {
	return h.authorize(func(p oidc.Principal) bool { return p.IsInternal() }, next)
}

func (h *handlers) requireProvider(scope string, next http.Handler) http.Handler {
	return h.authorize(func(p oidc.Principal) bool { return p.IsProvider() && p.Has(scope) }, next)
}

func (h *handlers) requireReader(scope string, next http.Handler) http.Handler {
	return h.authorize(func(p oidc.Principal) bool { return p.IsInternal() || p.IsProvider() && p.Has(scope) }, next)
}

func (h *handlers) authorize(allowed func(oidc.Principal) bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := oidc.PrincipalFrom(r.Context())
		if !ok {
			unauthenticated(w, r, `Bearer realm="wagering"`)
			return
		}
		if !allowed(p) {
			writeProblem(w, r, http.StatusForbidden, codeForbidden, "Forbidden",
				"the access token does not allow this operation", nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) wroteHeader() bool { return w.status != 0 }

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
