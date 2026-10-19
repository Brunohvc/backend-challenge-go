package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRun_UsageErrors(t *testing.T) {
	env := func(string) string { return "postgres://nobody@127.0.0.1:1/none" }
	for _, args := range [][]string{
		nil,
		{"sideways"},
		{"up", "2"},
		{"down"},
		{"down", "0"},
		{"down", "x"},
		{"down", "1", "2"},
		{"version", "6"},
		{"force"},
		{"force", "-2"},
		{"force", "v6"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(t.Context(), args, env, &stdout, &stderr); code != exitUsage {
			t.Errorf("run(%q) = %d, want %d", args, code, exitUsage)
		}
		if !strings.Contains(stderr.String(), "usage:") {
			t.Errorf("run(%q) printed no usage: %q", args, stderr.String())
		}
		if stdout.Len() != 0 {
			t.Errorf("run(%q) printed to stdout: %q", args, stdout.String())
		}
	}
}

func TestRun_MissingDatabaseURL(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), []string{"version"}, func(string) string { return "" }, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "MIGRATIONS_DATABASE_URL") {
		t.Fatalf("stderr = %q, want the variable named", stderr.String())
	}
}
