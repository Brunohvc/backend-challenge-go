package health_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/health"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func ok(context.Context) error { return nil }

func TestChecker_ReadyWhenEveryCheckPasses(t *testing.T) {
	c := health.NewChecker()
	c.Add("database", ok)
	c.Add("sqs", ok)

	ready, results := c.Ready(t.Context())

	if !ready {
		t.Errorf("Ready = false, want true (results %v)", results)
	}
	want := map[string]string{"database": "ok", "sqs": "ok"}
	if len(results) != len(want) || results["database"] != "ok" || results["sqs"] != "ok" {
		t.Errorf("results = %v, want %v", results, want)
	}
}

func TestChecker_NoChecksIsReady(t *testing.T) {
	ready, results := health.NewChecker().Ready(t.Context())

	if !ready || len(results) != 0 {
		t.Errorf("Ready = %v, %v; want true and no results", ready, results)
	}
}

func TestChecker_FailingCheck(t *testing.T) {
	c := health.NewChecker()
	c.Add("database", ok)
	c.Add("sqs", func(context.Context) error { return errors.New("queue unreachable") })

	ready, results := c.Ready(t.Context())

	if ready {
		t.Error("Ready = true with a failing check, want false")
	}
	if results["database"] != "ok" || results["sqs"] != "queue unreachable" {
		t.Errorf("results = %v, want database ok and the sqs error", results)
	}
}

func TestChecker_ChecksRunInParallel(t *testing.T) {
	c := health.NewChecker()
	const n = 3
	started := make(chan struct{}, n)
	release := make(chan struct{})
	for _, name := range []string{"a", "b", "c"} {
		c.Add(name, func(ctx context.Context) error {
			started <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}
	go func() {
		for range n {
			<-started
		}
		close(release)
	}()

	if ready, results := c.Ready(t.Context()); !ready {
		t.Errorf("Ready = false (%v), want the checks to run concurrently", results)
	}
}

func TestChecker_SlowCheckTimesOut(t *testing.T) {
	c := health.NewChecker()
	c.SetCheckTimeout(50 * time.Millisecond)
	unblock := make(chan struct{})
	t.Cleanup(func() { close(unblock) })
	c.Add("stuck", func(context.Context) error {
		<-unblock
		return nil
	})

	start := time.Now()
	ready, results := c.Ready(t.Context())

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Ready took %v, want it bounded by the check timeout", elapsed)
	}
	if ready || results["stuck"] == "ok" || results["stuck"] == "" {
		t.Errorf("Ready = %v, %v; want not ready with a timeout reported", ready, results)
	}
}

func TestChecker_ShuttingDown(t *testing.T) {
	c := health.NewChecker()
	called := false
	c.Add("database", func(context.Context) error {
		called = true
		return nil
	})

	c.SetShuttingDown()
	ready, results := c.Ready(t.Context())

	if ready {
		t.Error("Ready = true while shutting down, want false")
	}
	if _, ok := results["shutdown"]; !ok {
		t.Errorf("results = %v, want a shutdown entry", results)
	}
	if called {
		t.Error("checks ran while shutting down; the answer is known without them")
	}
}

func TestChecker_PanickingCheckFails(t *testing.T) {
	c := health.NewChecker()
	c.Add("database", ok)
	c.Add("broken", func(context.Context) error { panic("nil pointer in the probe") })

	ready, results := c.Ready(t.Context())

	if ready {
		t.Error("Ready = true with a panicking check, want false")
	}
	if results["database"] != "ok" || !strings.Contains(results["broken"], "panic") {
		t.Errorf("results = %v, want database ok and broken reporting the panic", results)
	}
}
