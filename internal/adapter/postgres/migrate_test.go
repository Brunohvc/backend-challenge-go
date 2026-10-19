package postgres

import (
	"errors"
	"io/fs"
	"strconv"
	"strings"
	"testing"

	"github.com/Brunohvc/backend-challenge-go/migrations"
)

func TestMigrate_RejectsInvalidArguments(t *testing.T) {
	const nowhere = "postgres://nobody@127.0.0.1:1/none"
	for _, tc := range []struct {
		direction string
		steps     int
	}{
		{"up", -1},
		{"down", 0},
		{"down", -2},
		{"force", -2},
		{"sideways", 1},
		{"", 0},
	} {
		err := Migrate(t.Context(), nowhere, tc.direction, tc.steps)
		if !errors.Is(err, ErrInvalidMigrationArgs) {
			t.Errorf("Migrate(%q, %d) = %v, want ErrInvalidMigrationArgs", tc.direction, tc.steps, err)
		}
	}
}

func TestLatestMigration_IsTheLastEmbeddedFile(t *testing.T) {
	names, err := fs.Glob(migrations.FS, "*.up.sql")
	if err != nil || len(names) == 0 {
		t.Fatalf("glob the embedded migrations: %v, %d files", err, len(names))
	}
	var want uint
	for _, name := range names {
		prefix, _, _ := strings.Cut(name, "_")
		v, err := strconv.ParseUint(prefix, 10, 64)
		if err != nil {
			t.Fatalf("migration %s has no numeric version: %v", name, err)
		}
		want = max(want, uint(v))
	}

	got, err := latestMigration()

	if err != nil {
		t.Fatalf("latestMigration() error = %v", err)
	}
	if got != want {
		t.Fatalf("latestMigration() = %d, want %d", got, want)
	}
}
