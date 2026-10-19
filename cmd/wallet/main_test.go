package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

const runMainVar = "WALLET_TEST_RUN_MAIN"

func TestMain_InvalidConfigurationExitsWithUsageStatus(t *testing.T) {
	if os.Getenv(runMainVar) == "1" {
		main()
		return
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestMain_InvalidConfigurationExitsWithUsageStatus$")
	cmd.Env = []string{runMainVar + "=1"}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	err := cmd.Run()

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != exitUsage {
		t.Fatalf("exit = %v, want status %d; stderr %q", err, exitUsage, stderr.String())
	}
	for _, want := range []string{"invalid configuration", "DATABASE_URL: is required", "OIDC_ISSUER: is required"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
		}
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want nothing", stdout.String())
	}
}
