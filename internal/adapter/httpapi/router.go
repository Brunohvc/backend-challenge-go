package httpapi

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
)

const healthPrefix = "/health/"

type handlers struct {
	deps Deps
}

func newHandlers(d Deps) *handlers {
	if d.IDs == nil {
		d.IDs = idgen.V7{}
	}
	return &handlers{deps: d}
}

type routeMux interface {
	http.Handler
	Handle(pattern string, handler http.Handler)
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

// NewRouter returns the API: every route behind the middleware chain.
func NewRouter(d Deps) http.Handler {
	return newRouter(d, http.NewServeMux())
}

func newRouter(d Deps, mux routeMux) http.Handler {
	h := newHandlers(d)
	h.routes(mux)
	return h.trace(h.middleware(mux))
}

func (h *handlers) trace(next http.Handler) http.Handler {
	tr := h.deps.Tracer
	if !tr.Enabled() {
		return next
	}
	return otelhttp.NewHandler(next, "http",
		otelhttp.WithTracerProvider(tr.Provider()),
		otelhttp.WithPropagators(tr.Propagator()),
		otelhttp.WithMeterProvider(noop.NewMeterProvider()),
		otelhttp.WithFilter(func(r *http.Request) bool { return !strings.HasPrefix(r.URL.Path, healthPrefix) }),
	)
}

func (h *handlers) routes(mux routeMux) {
	h.registerHealth(mux)
	h.registerWallets(mux)
	h.registerWagering(mux)
}

func (h *handlers) middleware(mux http.Handler) http.Handler {
	return h.recoverPanics(h.correlate(h.observe(answerUnmatched(mux))))
}

func answerUnmatched(mux http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(&unmatchedWriter{ResponseWriter: w, r: r}, r)
	})
}

type unmatchedWriter struct {
	http.ResponseWriter
	r        *http.Request
	replaced bool
}

func (w *unmatchedWriter) WriteHeader(code int) {
	switch {
	case w.r.Pattern != "":
		w.ResponseWriter.WriteHeader(code)
	case code == http.StatusNotFound:
		w.replaced = true
		writeProblem(w.ResponseWriter, w.r, code, codeNotFound, "Not found", "no route matches the path", nil)
	case code == http.StatusMethodNotAllowed:
		w.replaced = true
		writeProblem(w.ResponseWriter, w.r, code, codeMethodNotAllowed, "Method not allowed",
			"the path does not take this method; Allow lists the ones it takes", nil)
	default:
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *unmatchedWriter) Write(b []byte) (int, error) {
	if w.replaced {
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

func (w *unmatchedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
