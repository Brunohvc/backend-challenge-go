//go:build integration

package bootstrap_test

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"testing"
	"time"
)

type readiness struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

func (c *client) ready() (int, readiness) {
	c.t.Helper()
	code, raw := c.send(c.request(http.MethodGet, "/health/ready", nil))
	var r readiness
	if err := json.Unmarshal(raw, &r); err != nil {
		c.t.Fatalf("GET /health/ready = %d: decode %q: %v", code, raw, err)
	}
	return code, r
}

func TestApp_ReadinessFollowsTheBroker(t *testing.T) {
	app := newApp(t, appConfig(t, nil))
	app.RequireStart()
	defer app.RequireStop()
	api := newClient(t, "http://"+app.api.Addr())
	defer api.http.CloseIdleConnections()

	healthy := map[string]string{"postgres": "ok", "sqs": "ok"}
	if code, r := api.ready(); code != http.StatusOK || r.Status != "ok" || !maps.Equal(r.Checks, healthy) {
		t.Fatalf("GET /health/ready before the pause = %d %+v, want 200 with every check ok", code, r)
	}

	if err := env.AWS.Pause(t.Context()); err != nil {
		t.Fatalf("pause MiniStack: %v", err)
	}
	paused := true
	defer func() {
		if !paused {
			return
		}
		if err := env.AWS.Unpause(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("unpause MiniStack: %v", err)
		}
	}()

	brokerDown := map[string]string{"postgres": "ok", "sqs": "fail"}
	code, r := api.ready()
	if code != http.StatusServiceUnavailable || r.Status != "unavailable" || !maps.Equal(r.Checks, brokerDown) {
		t.Fatalf("GET /health/ready with MiniStack paused = %d %+v, want 503 with sqs fail and postgres ok", code, r)
	}

	if err := env.AWS.Unpause(t.Context()); err != nil {
		t.Fatalf("unpause MiniStack: %v", err)
	}
	paused = false
	const recovery = 10 * time.Second
	deadline := time.Now().Add(recovery)
	for {
		code, r = api.ready()
		if code == http.StatusOK && r.Status == "ok" && maps.Equal(r.Checks, healthy) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET /health/ready %s after the unpause = %d %+v, want 200 with every check ok", recovery, code, r)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
