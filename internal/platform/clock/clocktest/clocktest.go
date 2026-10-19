// Package clocktest provides a clock that tests drive by hand.
package clocktest

import (
	"sync"
	"time"
)

// Fixed is a manually driven clock, safe for concurrent use.
type Fixed struct {
	T time.Time

	mu sync.Mutex
}

// Now returns the configured time.
func (f *Fixed) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.T
}

// Advance moves the clock forward by d.
func (f *Fixed) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.T = f.T.Add(d)
}

// Set moves the clock to t, which may be before the time it shows.
func (f *Fixed) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.T = t
}
