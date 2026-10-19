package clock_test

import (
	"testing"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/clock"
)

func TestSystem_NowIsUTC(t *testing.T) {
	got := clock.System{}.Now()

	if got.Location() != time.UTC {
		t.Errorf("location = %v, want UTC", got.Location())
	}
}
