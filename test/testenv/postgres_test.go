//go:build integration || e2e

package testenv

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPostgresImageMatchesCompose(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	compose, err := os.ReadFile(filepath.Join(root, "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(compose), "image: "+postgresImage+"\n") {
		t.Fatalf("docker-compose.yml does not pin %s", postgresImage)
	}
}
