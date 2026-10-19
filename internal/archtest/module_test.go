package archtest

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func moduleRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Stat(go.mod) in %s error = %v", dir, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found in any parent of the working directory")
		}
		dir = parent
	}
}

func goList(t *testing.T, dir string, args ...string) string {
	t.Helper()

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(t.Context(), "go", append([]string{"list"}, args...)...)
	cmd.Dir = dir
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go list %s error = %v\n%s", strings.Join(args, " "), err, stderr.String())
	}
	return stdout.String()
}

func within(p, dir string) bool {
	return p == dir || strings.HasPrefix(p, dir+"/")
}
