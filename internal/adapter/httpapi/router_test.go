package httpapi

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/logging"
)

func TestNewRouter_WiresMiddlewareAndDefaults(t *testing.T) {
	m := &fakeMetrics{}
	h := NewRouter(Deps{
		Verifier:     fakeVerifier{principals: principals},
		Health:       fakeHealth{ready: true},
		Metrics:      m,
		Log:          logging.New(&bytes.Buffer{}, 0, "test"),
		MaxBodyBytes: 1024,
	})
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health/live", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	id, err := uuid.Parse(rec.Header().Get(headerCorrelationID))
	if err != nil || id.Version() != 7 {
		t.Errorf("X-Correlation-Id = %q, want a generated UUIDv7", rec.Header().Get(headerCorrelationID))
	}
	if len(m.obs) != 1 || m.obs[0] != (observation{"GET /health/live", http.MethodGet, http.StatusOK}) {
		t.Errorf("observations = %+v, want one for GET /health/live 200", m.obs)
	}
}

var publicRoutes = []string{"GET /health/live", "GET /health/ready"}

type recordingMux struct {
	*http.ServeMux
	patterns []string
}

func (m *recordingMux) Handle(pattern string, handler http.Handler) {
	m.patterns = append(m.patterns, pattern)
	m.ServeMux.Handle(pattern, handler)
}

func (m *recordingMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	m.patterns = append(m.patterns, pattern)
	m.ServeMux.HandleFunc(pattern, handler)
}

func TestNewRouter_EveryRouteButHealthRequiresAToken(t *testing.T) {
	m := &fakeMetrics{}
	d := Deps{
		Verifier:     fakeVerifier{principals: principals},
		Health:       fakeHealth{ready: true},
		Metrics:      m,
		Log:          logging.New(&bytes.Buffer{}, 0, "test"),
		MaxBodyBytes: 1024,
		IDs:          fixedIDs{},
	}
	inventory := &recordingMux{ServeMux: http.NewServeMux()}
	router := newRouter(d, inventory)

	for _, pattern := range publicRoutes {
		if !slices.Contains(inventory.patterns, pattern) {
			t.Errorf("public route %q is not registered", pattern)
		}
	}
	protected := slices.DeleteFunc(slices.Clone(inventory.patterns), func(p string) bool { return slices.Contains(publicRoutes, p) })
	if len(protected) == 0 {
		t.Fatalf("routes = %v, want some besides the public ones", inventory.patterns)
	}
	for _, pattern := range protected {
		t.Run(pattern, func(t *testing.T) {
			method, target := requestFor(pattern)
			req := httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(`{}`))
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			requireProblem(t, rec, http.StatusUnauthorized, codeUnauthenticated)
			if challenge := rec.Header().Get("WWW-Authenticate"); !strings.HasPrefix(challenge, "Bearer ") {
				t.Errorf("WWW-Authenticate = %q, want a Bearer challenge", challenge)
			}
			if got := m.last(t).route; got != pattern {
				t.Errorf("request %s %s was routed to %q, want %q", method, target, got, pattern)
			}
		})
	}
}

var wildcard = regexp.MustCompile(`\{[^}$]*\}`)

func requestFor(pattern string) (method, target string) {
	method, target, ok := strings.Cut(pattern, " ")
	if !ok {
		method, target = http.MethodGet, pattern
	}
	target = strings.ReplaceAll(wildcard.ReplaceAllString(target, generatedIDText), "{$}", "")
	return method, target
}

func TestRouter_UnmatchedRequestIsAProblem(t *testing.T) {
	api := newTestAPI(t)

	tests := []struct {
		name, method, target string
		status               int
		code, allow          string
	}{
		{"unknown path", http.MethodGet, "/nope", http.StatusNotFound, codeNotFound, ""},
		{"path below a route", http.MethodGet, "/health/live/more", http.StatusNotFound, codeNotFound, ""},
		{"known path, other method", http.MethodDelete, "/health/live", http.StatusMethodNotAllowed, codeMethodNotAllowed, "GET, HEAD"},
		{"nonstandard method", "BREW", "/test/provider", http.StatusMethodNotAllowed, codeMethodNotAllowed, "GET, HEAD, POST"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := api.do(t, tt.method, tt.target, "", map[string]string{"X-Correlation-Id": "c-unmatched"})

			p := requireProblem(t, rec, tt.status, tt.code)
			if p["correlationId"] != "c-unmatched" {
				t.Errorf("correlationId = %v, want c-unmatched", p["correlationId"])
			}
			if got := rec.Header().Get("Allow"); got != tt.allow {
				t.Errorf("Allow = %q, want %q", got, tt.allow)
			}
		})
	}
}

func TestRouter_HandlerProblemIsKept(t *testing.T) {
	h := newHandlers(Deps{Log: logging.New(&bytes.Buffer{}, 0, "test"), Metrics: &fakeMetrics{}, IDs: fixedIDs{}})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /gone", func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, r, http.StatusNotFound, codeWalletNotFound, "Wallet not found", "", nil)
	})
	rec := httptest.NewRecorder()

	h.middleware(mux).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/gone", nil))

	requireProblem(t, rec, http.StatusNotFound, codeWalletNotFound)
}
