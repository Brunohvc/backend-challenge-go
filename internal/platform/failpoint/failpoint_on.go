//go:build failpoints

package failpoint

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
)

const killedExitCode = 137

const malformedExitCode = 2

var known = []string{
	SubmitBeforeCommit, SubmitAfterCommit, ConsumerAfterCommitBeforeDelete,
	OutboxAfterClaim, OutboxAfterPublishBeforeAck, ResolverAfterClaim,
}

var (
	load  sync.Once
	armed map[string]bool
)

// Inject ends the process with status 137 when FAILPOINTS arms name.
func Inject(name string) {
	load.Do(func() {
		var err error
		if armed, err = parseSetting(os.Getenv("FAILPOINTS")); err != nil {
			fmt.Fprintf(os.Stderr, "failpoint: invalid FAILPOINTS: %v\n", err)
			os.Exit(malformedExitCode)
		}
	})
	if armed[name] {
		os.Exit(killedExitCode)
	}
}

func parseSetting(setting string) (map[string]bool, error) {
	points := make(map[string]bool)
	if strings.TrimSpace(setting) == "" {
		return points, nil
	}
	for entry := range strings.SplitSeq(setting, ",") {
		name, action, ok := strings.Cut(strings.TrimSpace(entry), "=")
		switch {
		case !ok || name == "":
			return nil, fmt.Errorf("entry %q is not name=exit", entry)
		case action != "exit":
			return nil, fmt.Errorf("entry %q: unknown action %q, want exit", entry, action)
		case !slices.Contains(known, name):
			return nil, fmt.Errorf("entry %q: unknown failpoint %q", entry, name)
		case points[name]:
			return nil, errors.New("failpoint " + name + " is armed twice")
		}
		points[name] = true
	}
	return points, nil
}
