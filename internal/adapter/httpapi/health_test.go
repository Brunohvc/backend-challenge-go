package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/health"
)

func decodeBody(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return m
}

func TestHealth_Live(t *testing.T) {
	rec := newTestAPI(t).do(t, http.MethodGet, "/health/live", "", nil)

	if rec.Code != http.StatusOK || decodeBody(t, rec.Body.Bytes())["status"] != "ok" {
		t.Errorf("live = %d %s, want 200 {\"status\":\"ok\"}", rec.Code, rec.Body)
	}
}

func TestHealth_Ready(t *testing.T) {
	c := health.NewChecker()
	c.Add("database", func(context.Context) error { return nil })
	api := newTestAPI(t, func(d *Deps) { d.Health = c })

	rec := api.do(t, http.MethodGet, "/health/ready", "", nil)

	body := decodeBody(t, rec.Body.Bytes())
	checks, _ := body["checks"].(map[string]any)
	if rec.Code != http.StatusOK || body["status"] != "ok" || checks["database"] != "ok" {
		t.Errorf("ready = %d %s, want 200 with database ok", rec.Code, rec.Body)
	}
}

func TestHealth_ReadyReportsFailingCheck(t *testing.T) {
	c := health.NewChecker()
	c.Add("database", func(context.Context) error { return nil })
	c.Add("sqs", func(context.Context) error { return errors.New("dial tcp 10.0.0.7:4566: connection refused") })
	api := newTestAPI(t, func(d *Deps) { d.Health = c })

	rec := api.do(t, http.MethodGet, "/health/ready", "", nil)

	body := decodeBody(t, rec.Body.Bytes())
	checks, _ := body["checks"].(map[string]any)
	if rec.Code != http.StatusServiceUnavailable || body["status"] != "unavailable" ||
		checks["database"] != "ok" || checks["sqs"] != checkFailed {
		t.Errorf("ready = %d %s, want 503 with sqs %s", rec.Code, rec.Body, checkFailed)
	}
	if want := "10.0.0.7"; strings.Contains(rec.Body.String(), want) || !strings.Contains(api.logs.String(), want) {
		t.Errorf("body %s / log %s: the failure detail must be logged and not returned", rec.Body, api.logs)
	}
}

func TestHealth_ShuttingDown503(t *testing.T) {
	c := health.NewChecker()
	c.Add("database", func(context.Context) error { return nil })
	c.SetShuttingDown()
	api := newTestAPI(t, func(d *Deps) { d.Health = c })

	rec := api.do(t, http.MethodGet, "/health/ready", "", nil)

	if rec.Code != http.StatusServiceUnavailable || decodeBody(t, rec.Body.Bytes())["status"] != "unavailable" {
		t.Errorf("ready = %d %s, want 503 while shutting down", rec.Code, rec.Body)
	}
	if rec := api.do(t, http.MethodGet, "/health/live", "", nil); rec.Code != http.StatusOK {
		t.Errorf("live = %d while shutting down, want 200", rec.Code)
	}
}
