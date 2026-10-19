//go:build !failpoints

package failpoint_test

import (
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/failpoint"
)

func TestFailpoint_NoopWithoutTag(t *testing.T) {
	t.Setenv("FAILPOINTS", failpoint.SubmitBeforeCommit+"=exit,"+failpoint.OutboxAfterClaim+"=exit")

	for _, name := range []string{failpoint.SubmitBeforeCommit, failpoint.OutboxAfterClaim, "not.a.failpoint"} {
		failpoint.Inject(name)
	}
}
