package httpapi

import (
	"encoding/base64"
	"math"
	"testing"
)

func TestCursor_RoundTripAndTamper(t *testing.T) {
	for _, seq := range []int64{0, 1, 42, math.MaxInt64} {
		got, err := decodeCursor(encodeCursor(seq))
		if err != nil || got != seq {
			t.Errorf("decodeCursor(encodeCursor(%d)) = %d, %v", seq, got, err)
		}
	}

	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	tampered := map[string]string{
		"empty":             "",
		"not base64":        "!!!",
		"padded base64":     base64.URLEncoding.EncodeToString([]byte(`{"v":1,"seq":10}`)),
		"not JSON":          enc("seq=1"),
		"other version":     enc(`{"v":2,"seq":1}`),
		"no version":        enc(`{"seq":1}`),
		"negative seq":      enc(`{"v":1,"seq":-1}`),
		"fractional seq":    enc(`{"v":1,"seq":1.5}`),
		"seq as string":     enc(`{"v":1,"seq":"1"}`),
		"unknown field":     enc(`{"v":1,"seq":1,"x":0}`),
		"keys reordered":    enc(`{"seq":1,"v":1}`),
		"whitespace":        enc(`{"v":1, "seq":1}`),
		"trailing data":     enc(`{"v":1,"seq":1}{}`),
		"one character off": encodeCursor(7)[:len(encodeCursor(7))-1] + "x",
		"seq beyond int64":  enc(`{"v":1,"seq":9223372036854775808}`),
	}
	for name, cursor := range tampered {
		t.Run(name, func(t *testing.T) {
			if seq, err := decodeCursor(cursor); err == nil {
				t.Errorf("decodeCursor(%q) = %d, want an error", cursor, seq)
			}
		})
	}
}
