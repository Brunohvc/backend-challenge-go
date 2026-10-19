//go:build integration || e2e

package testenv

import (
	"bytes"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

// ServiceConfig loads the configuration of an in-process service against e, with overrides.
func (e *Env) ServiceConfig(t testing.TB, instanceID string, overrides map[string]string) config.Config {
	t.Helper()
	vars := e.AppEnv(instanceID, overrides)
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if _, set := vars[name]; set || !strings.HasPrefix(name, "AWS_") {
			continue
		}
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("testenv: unset %s: %v", name, err)
		}
	}
	for name, value := range vars {
		if strings.HasPrefix(name, "AWS_") {
			t.Setenv(name, value)
		}
	}
	cfg, err := config.Load(func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	})
	if err != nil {
		t.Fatalf("testenv: load the service configuration: %v", err)
	}
	return cfg
}

// SyncBuffer is a bytes.Buffer safe for concurrent writes.
type SyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends p to the buffer.
func (b *SyncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// String returns everything written so far.
func (b *SyncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
