package idgen_test

import (
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/idgen"
)

func TestV7_GeneratesTimeOrderedVersion7(t *testing.T) {
	var gen idgen.V7

	a, b := gen.New(), gen.New()

	if a.Version() != 7 || b.Version() != 7 {
		t.Fatalf("versions = %d and %d, want 7", a.Version(), b.Version())
	}
	if a.String() >= b.String() {
		t.Errorf("ids are not time ordered: %s >= %s", a, b)
	}
}
