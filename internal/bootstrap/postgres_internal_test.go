package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/app"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing"
)

func TestDatabase_RefusedOutsideItsLifetime(t *testing.T) {
	d := newDatabase(config.Config{}, tracing.Tracer{})
	noop := func(context.Context, app.Repos) error { return nil }

	uses := map[string]func() error{
		"Do":       func() error { return d.Do(t.Context(), noop) },
		"Snapshot": func() error { return d.Snapshot(t.Context(), noop) },
		"ping":     func() error { return d.ping(t.Context()) },
	}
	for name, use := range uses {
		if err := use(); !errors.Is(err, errDatabaseNotStarted) {
			t.Errorf("%s before start = %v, want errDatabaseNotStarted", name, err)
		}
	}
	if err := d.stop(t.Context()); err != nil {
		t.Errorf("stop before start = %v, want nil", err)
	}
}
