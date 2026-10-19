package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/adapter/oidc"
)

func TestAuth_MissingToken(t *testing.T) {
	api := newTestAPI(t)

	for name, headers := range map[string]map[string]string{
		"no header":     nil,
		"other scheme":  {"Authorization": "Basic cHJvdmlkZXItYTpzZWNyZXQ="},
		"empty bearer":  {"Authorization": "Bearer "},
		"scheme only":   {"Authorization": "Bearer"},
		"unknown token": bearer("forged"),
	} {
		t.Run(name, func(t *testing.T) {
			rec := api.do(t, http.MethodGet, "/test/protected", "", headers)

			requireProblem(t, rec, http.StatusUnauthorized, codeUnauthenticated)
			if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Bearer") {
				t.Errorf("WWW-Authenticate = %q, want a Bearer challenge", rec.Header().Get("WWW-Authenticate"))
			}
			if api.reached["protected"] {
				t.Error("the handler ran without a valid token")
			}
		})
	}
}

func TestAuth_SchemeIsCaseInsensitive(t *testing.T) {
	api := newTestAPI(t)

	rec := api.do(t, http.MethodGet, "/test/protected", "", map[string]string{"Authorization": "bearer " + providerToken})

	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204 (body %s)", rec.Code, rec.Body)
	}
}

func TestAuth_RejectedTokens(t *testing.T) {
	unknownKey := fmt.Errorf("%w: %w", oidc.ErrInvalidToken, oidc.ErrUnknownKey)
	for _, verdict := range []error{oidc.ErrInvalidToken, unknownKey, oidc.ErrExpiredToken, oidc.ErrMisconfiguredPrincipal, oidc.ErrMissingToken} {
		t.Run(verdict.Error(), func(t *testing.T) {
			api := newTestAPI(t, func(d *Deps) {
				d.Verifier = fakeVerifier{err: fmt.Errorf("wrapped: %w", verdict)}
			})

			rec := api.do(t, http.MethodGet, "/test/protected", "", bearer("whatever"))

			requireProblem(t, rec, http.StatusUnauthorized, codeUnauthenticated)
			if api.reached["protected"] {
				t.Error("the handler ran with a rejected token")
			}
		})
	}
}

func TestAuth_VerifierFailureIsUnavailable(t *testing.T) {
	api := newTestAPI(t, func(d *Deps) { d.Verifier = fakeVerifier{err: errors.New("verifier not started")} })

	rec := api.do(t, http.MethodGet, "/test/protected", "", bearer("whatever"))

	requireProblem(t, rec, http.StatusServiceUnavailable, codeTemporarilyUnavailable)
	if rec.Header().Get("Retry-After") == "" {
		t.Error("no Retry-After on 503")
	}
}

func TestRequireInternal(t *testing.T) {
	api := newTestAPI(t)

	t.Run("internal", func(t *testing.T) {
		if rec := api.do(t, http.MethodGet, "/test/internal", "", bearer(internalToken)); rec.Code != http.StatusNoContent {
			t.Errorf("status = %d, want 204", rec.Code)
		}
	})
	t.Run("provider", func(t *testing.T) {
		delete(api.reached, "internal")
		rec := api.do(t, http.MethodGet, "/test/internal", "", bearer(providerToken))

		requireProblem(t, rec, http.StatusForbidden, codeForbidden)
		if api.reached["internal"] {
			t.Error("a provider reached an internal route")
		}
	})
}

func TestRequireProvider(t *testing.T) {
	api := newTestAPI(t)

	tests := []struct {
		name   string
		token  string
		status int
	}{
		{"provider with the scope", providerToken, http.StatusNoContent},
		{"provider without the scope", readOnlyToken, http.StatusForbidden},
		{"internal", internalToken, http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delete(api.reached, "provider")
			rec := api.do(t, http.MethodGet, "/test/provider", "", bearer(tt.token))

			if tt.status == http.StatusForbidden {
				requireProblem(t, rec, http.StatusForbidden, codeForbidden)
				if api.reached["provider"] {
					t.Error("the handler ran for a principal without access")
				}
				return
			}
			if rec.Code != tt.status {
				t.Errorf("status = %d, want %d", rec.Code, tt.status)
			}
		})
	}
}

func TestRequireReader(t *testing.T) {
	api := newTestAPI(t)

	tests := []struct {
		name   string
		token  string
		status int
	}{
		{"internal", internalToken, http.StatusNoContent},
		{"provider with the scope", readOnlyToken, http.StatusNoContent},
		{"provider without the scope", writeOnlyToken, http.StatusForbidden},
		{"client of no provider with the scope", rolelessToken, http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delete(api.reached, "reader")
			rec := api.do(t, http.MethodGet, "/test/reader", "", bearer(tt.token))

			if tt.status == http.StatusForbidden {
				requireProblem(t, rec, http.StatusForbidden, codeForbidden)
				if api.reached["reader"] {
					t.Error("the handler ran for a principal without access")
				}
				return
			}
			if rec.Code != tt.status || !api.reached["reader"] {
				t.Errorf("status = %d, handler reached %t; want %d and the handler", rec.Code, api.reached["reader"], tt.status)
			}
		})
	}
}

func TestCorrelation_EchoAndGenerate(t *testing.T) {
	api := newTestAPI(t)

	tests := []struct {
		name, sent, want string
	}{
		{"valid id is echoed", "c-0192f298-345e", "c-0192f298-345e"},
		{"missing id is generated", "", generatedIDText},
		{"invalid id is replaced", "bad id!", generatedIDText},
		{"overlong id is replaced", strings.Repeat("a", 129), generatedIDText},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api.logs.Reset()
			headers := map[string]string{}
			if tt.sent != "" {
				headers["X-Correlation-Id"] = tt.sent
			}

			rec := api.do(t, http.MethodGet, "/test/protected", "", headers)

			if got := rec.Header().Get("X-Correlation-Id"); got != tt.want {
				t.Errorf("X-Correlation-Id = %q, want %q", got, tt.want)
			}
			if p := problemOf(t, rec); p["correlationId"] != tt.want {
				t.Errorf("problem correlationId = %v, want %q", p["correlationId"], tt.want)
			}
			if !strings.Contains(api.logs.String(), `"correlationId":"`+tt.want+`"`) {
				t.Errorf("access log does not carry the correlation id:\n%s", api.logs)
			}
		})
	}
}

func TestRecover_Returns500Problem(t *testing.T) {
	api := newTestAPI(t)

	rec := api.do(t, http.MethodGet, "/test/panic", "", map[string]string{"X-Correlation-Id": "c-panic"})

	p := requireProblem(t, rec, http.StatusInternalServerError, codeInternalError)
	if p["correlationId"] != "c-panic" {
		t.Errorf("correlationId = %v, want c-panic", p["correlationId"])
	}
	if strings.Contains(rec.Body.String(), "boom") {
		t.Error("the panic value leaked into the response")
	}
	logs := api.logs.String()
	if !strings.Contains(logs, "boom") || !strings.Contains(logs, "goroutine") {
		t.Errorf("log does not record the panic and its stack:\n%s", logs)
	}
	if o := api.metrics.last(t); o.code != http.StatusInternalServerError || o.route != "GET /test/panic" {
		t.Errorf("observed %+v, want GET /test/panic with 500", o)
	}
}

func TestAccessLog_NeverLogsSecretsOrBodies(t *testing.T) {
	api := newTestAPI(t)
	const secret = "token-provider-a"

	rec := api.do(t, http.MethodPost, "/test/provider", `{"money":{"amount":"987.65","currency":"BRL"}}`, map[string]string{
		"Authorization":    "Bearer " + secret,
		"Content-Type":     "application/json",
		"X-Correlation-Id": "c-log-1",
	})

	if rec.Code != http.StatusCreated || !strings.Contains(rec.Body.String(), "987.65") {
		t.Fatalf("status = %d body %s, want 201 echoing the amount (the request must reach the handler)", rec.Code, rec.Body)
	}
	logs := api.logs.String()
	for _, leaked := range []string{secret, "987.65", "Bearer", "Authorization"} {
		if strings.Contains(logs, leaked) {
			t.Errorf("log contains %q:\n%s", leaked, logs)
		}
	}
	for _, want := range []string{`"correlationId":"c-log-1"`, `"providerId":"provider-a"`, `"route":"POST /test/provider"`, `"status":201`} {
		if !strings.Contains(logs, want) {
			t.Errorf("log lacks %s:\n%s", want, logs)
		}
	}
}

func TestMetrics_UnmatchedRouteUsesPlaceholder(t *testing.T) {
	api := newTestAPI(t)

	tests := []struct {
		name, method, target string
		want                 observation
	}{
		{"matched route uses its pattern", http.MethodGet, "/test/protected", observation{"GET /test/protected", http.MethodGet, http.StatusUnauthorized}},
		{"unknown path", http.MethodGet, "/wallets/0192f291-27dd-7d3f-8071-5f8685deef37/secret", observation{routeUnmatched, http.MethodGet, http.StatusNotFound}},
		{"known path, other method", http.MethodDelete, "/health/live", observation{routeUnmatched, http.MethodDelete, http.StatusMethodNotAllowed}},
		{"nonstandard method", "BREW", "/health/live", observation{routeUnmatched, methodOther, http.StatusMethodNotAllowed}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api.do(t, tt.method, tt.target, "", nil)

			if got := api.metrics.last(t); got != tt.want {
				t.Errorf("observed %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestAuth_LogsTheFailureClassNotTheToken(t *testing.T) {
	const secret = "eyJhbGciOiJSUzI1NiJ9.secret-payload.signature"

	tests := []struct {
		name, header, reason, level string
		verdict                     error
	}{
		{"missing", "", "missing", "DEBUG", nil},
		{"invalid", "Bearer " + secret, "invalid", "DEBUG", oidc.ErrInvalidToken},
		{"expired", "Bearer " + secret, "expired", "DEBUG", oidc.ErrExpiredToken},
		{"misconfigured", "Bearer " + secret, "misconfigured", "WARN", oidc.ErrMisconfiguredPrincipal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newTestAPI(t, func(d *Deps) { d.Verifier = fakeVerifier{err: tt.verdict} })
			headers := map[string]string{}
			if tt.header != "" {
				headers["Authorization"] = tt.header
			}

			api.do(t, http.MethodGet, "/test/protected", "", headers)

			line := logLine(t, api.logs.String(), "authentication failed")
			if line["reason"] != tt.reason || line["level"] != tt.level {
				t.Errorf("log = %v, want reason %s at %s", line, tt.reason, tt.level)
			}
			if strings.Contains(api.logs.String(), secret) || strings.Contains(api.logs.String(), "secret-payload") {
				t.Errorf("log contains the token:\n%s", api.logs)
			}
		})
	}
}

func logLine(t *testing.T, logs, msg string) map[string]any {
	t.Helper()
	for line := range strings.Lines(logs) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err == nil && m["msg"] == msg {
			return m
		}
	}
	t.Fatalf("no %q log line in:\n%s", msg, logs)
	return nil
}
