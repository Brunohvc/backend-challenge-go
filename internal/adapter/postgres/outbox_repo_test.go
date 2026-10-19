package postgres

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestLastError(t *testing.T) {
	ascii := strings.Repeat("a", lastErrorLimit+5)
	wide := strings.Repeat("é", lastErrorLimit-1) + "€😀"
	tests := []struct {
		name, in, want string
	}{
		{"a short error is kept", "connection reset", "connection reset"},
		{"an error at the limit is kept", ascii[:lastErrorLimit], ascii[:lastErrorLimit]},
		{"a longer one is cut at the limit", ascii, ascii[:lastErrorLimit]},
		{"the cut falls between characters", wide, strings.Repeat("é", lastErrorLimit-1) + "€"},
		{"invalid UTF-8 is replaced", "bad \xff\xfe byte", "bad � byte"},
		{"NUL is dropped", "nul\x00byte", "nulbyte"},
		{"nothing stays nothing", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lastError(tt.in)
			if got != tt.want {
				t.Fatalf("lastError(%q) = %q, want %q", tt.in, got, tt.want)
			}
			if !utf8.ValidString(got) || utf8.RuneCountInString(got) > lastErrorLimit {
				t.Fatalf("lastError(%q) = %q: invalid or longer than %d characters", tt.in, got, lastErrorLimit)
			}
		})
	}
}
