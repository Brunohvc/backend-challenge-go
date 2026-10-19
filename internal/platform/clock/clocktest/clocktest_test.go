package clocktest_test

import (
	"sync"
	"testing"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/clock/clocktest"
)

func TestFixed_NowReturnsConfiguredTime(t *testing.T) {
	start := time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)
	c := &clocktest.Fixed{T: start}

	if got := c.Now(); !got.Equal(start) {
		t.Errorf("Now() = %v, want %v", got, start)
	}
}

func TestFixed_AdvanceMovesTime(t *testing.T) {
	start := time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)
	c := &clocktest.Fixed{T: start}

	c.Advance(90 * time.Second)

	want := start.Add(90 * time.Second)
	if got := c.Now(); !got.Equal(want) {
		t.Errorf("Now() = %v, want %v", got, want)
	}
}

func TestFixed_SetMovesTime(t *testing.T) {
	c := &clocktest.Fixed{T: time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)}
	earlier := time.Date(2026, time.October, 1, 8, 30, 0, 0, time.UTC)

	c.Set(earlier)

	if got := c.Now(); !got.Equal(earlier) {
		t.Errorf("Now() = %v, want %v", got, earlier)
	}
}

func TestFixed_ConcurrentUse(t *testing.T) {
	start := time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)
	c := &clocktest.Fixed{T: start}
	const readers, moves = 4, 100

	var wg sync.WaitGroup
	for range readers {
		wg.Go(func() {
			for range moves {
				if now := c.Now(); now.Before(start) {
					t.Errorf("Now() = %v, before the start %v", now, start)
				}
			}
		})
	}
	for i := range moves {
		c.Advance(time.Second)
		if i%10 == 0 {
			c.Set(c.Now())
		}
	}
	wg.Wait()

	if got, want := c.Now(), start.Add(moves*time.Second); !got.Equal(want) {
		t.Errorf("Now() = %v, want %v", got, want)
	}
}
