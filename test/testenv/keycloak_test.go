//go:build integration || e2e

package testenv

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDevRealm_AllowsPlainHTTP(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "deploy", "keycloak", "realm-wagering.json"))
	if err != nil {
		t.Fatal(err)
	}

	var realm struct {
		SSLRequired string `json:"sslRequired"`
	}
	if err := json.Unmarshal(raw, &realm); err != nil {
		t.Fatalf("decode realm: %v", err)
	}

	if realm.SSLRequired != "none" {
		t.Errorf("sslRequired = %q, want %q", realm.SSLRequired, "none")
	}
}
