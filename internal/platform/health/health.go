// Package health decides whether an instance is ready from named dependency checks.
package health

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

const defaultCheckTimeout = 2 * time.Second

// Check reports whether one dependency is usable. It must honor ctx.
type Check func(ctx context.Context) error

// Checker runs named checks and is safe for concurrent use.
type Checker struct {
	timeout      time.Duration
	shuttingDown atomic.Bool

	mu     sync.RWMutex
	checks map[string]Check
}

// NewChecker returns a Checker with no checks, ready until SetShuttingDown.
func NewChecker() *Checker {
	return &Checker{timeout: defaultCheckTimeout, checks: make(map[string]Check)}
}

// Add registers chk under name, replacing any check of that name.
func (c *Checker) Add(name string, chk Check) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checks[name] = chk
}

// SetShuttingDown makes every later Ready report not ready.
func (c *Checker) SetShuttingDown() {
	c.shuttingDown.Store(true)
}

// Ready runs every check in parallel and reports whether all passed, with the result of each.
func (c *Checker) Ready(ctx context.Context) (bool, map[string]string) {
	if c.shuttingDown.Load() {
		return false, map[string]string{"shutdown": "in progress"}
	}
	c.mu.RLock()
	checks := maps.Clone(c.checks)
	c.mu.RUnlock()

	type result struct{ name, status string }
	out := make(chan result, len(checks))
	for name, chk := range checks {
		go func() {
			out <- result{name, c.run(ctx, chk)}
		}()
	}
	ready := true
	results := make(map[string]string, len(checks))
	for range checks {
		r := <-out
		results[r.name] = r.status
		ready = ready && r.status == "ok"
	}
	return ready, results
}

func (c *Checker) run(ctx context.Context, chk Check) string {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		defer func() {
			if v := recover(); v != nil {
				done <- fmt.Errorf("check panicked: %v", v)
			}
		}()
		done <- chk(ctx)
	}()
	select {
	case err := <-done:
		if err != nil {
			return err.Error()
		}
		return "ok"
	case <-ctx.Done():
		return fmt.Sprintf("no answer within %v", c.timeout)
	}
}
