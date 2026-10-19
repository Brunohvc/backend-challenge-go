//go:build failpoints

package failpoint

import (
	"errors"
	"os"
	"os/exec"
	"testing"
)

const (
	helperEnv     = "FAILPOINT_HELPER_PROCESS"
	helperNameEnv = "FAILPOINT_HELPER_NAME"
)

func TestHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		t.Skip("only runs as the helper process of TestInject")
	}
	Inject(os.Getenv(helperNameEnv))
	os.Exit(0)
}

func runHelper(t *testing.T, setting, name string) (int, string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(), helperEnv+"=1", helperNameEnv+"="+name, "FAILPOINTS="+setting, "GORACE=atexit_sleep_ms=0")
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), string(out)
	default:
		t.Fatalf("run helper: %v", err)
		return 0, ""
	}
}

func TestInject(t *testing.T) {
	tests := []struct {
		name     string
		setting  string
		inject   string
		wantCode int
	}{
		{"configured failpoint kills the process", SubmitBeforeCommit + "=exit", SubmitBeforeCommit, 137},
		{"one of several", SubmitAfterCommit + "=exit," + OutboxAfterClaim + "=exit", OutboxAfterClaim, 137},
		{"other failpoint returns", SubmitAfterCommit + "=exit", SubmitBeforeCommit, 0},
		{"nothing configured returns", "", ResolverAfterClaim, 0},
		{"malformed setting fails loudly", SubmitBeforeCommit, SubmitBeforeCommit, 2},
		{"unknown action fails loudly", SubmitBeforeCommit + "=sleep(1s)", ResolverAfterClaim, 2},
		{"misspelled name fails loudly", "submit.befor_commit=exit", SubmitBeforeCommit, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out := runHelper(t, tt.setting, tt.inject)

			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d (output %q)", code, tt.wantCode, out)
			}
		})
	}
}

func TestParseSetting(t *testing.T) {
	got, err := parseSetting(" " + SubmitBeforeCommit + "=exit , " + OutboxAfterPublishBeforeAck + "=exit ")
	if err != nil {
		t.Fatalf("parseSetting: %v", err)
	}
	if !got[SubmitBeforeCommit] || !got[OutboxAfterPublishBeforeAck] || len(got) != 2 {
		t.Errorf("parseSetting = %v, want both failpoints armed", got)
	}

	for _, bad := range []string{
		"=exit",
		SubmitBeforeCommit,
		SubmitBeforeCommit + "=",
		SubmitBeforeCommit + "=panic",
		SubmitBeforeCommit + "=exit,",
		SubmitBeforeCommit + "=exit," + SubmitBeforeCommit + "=exit",
		"unknown.point=exit",
	} {
		if _, err := parseSetting(bad); err == nil {
			t.Errorf("parseSetting(%q) succeeded, want an error", bad)
		}
	}

	if got, err := parseSetting(""); err != nil || len(got) != 0 {
		t.Errorf("parseSetting(\"\") = %v, %v; want nothing armed", got, err)
	}
}
