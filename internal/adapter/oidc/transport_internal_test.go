package oidc

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestKeySetTransport_OnlyA200ClearsTheFlag(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		err       error
		wantClear bool
	}{
		{"200 clears", http.StatusOK, nil, true},
		{"204 keeps", http.StatusNoContent, nil, false},
		{"500 keeps", http.StatusInternalServerError, nil, false},
		{"transport error keeps", 0, errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var failing atomic.Bool
			failing.Store(true)
			tr := keySetTransport{failing: &failing, base: roundTripFunc(func(*http.Request) (*http.Response, error) {
				if tt.err != nil {
					return nil, tt.err
				}
				return &http.Response{StatusCode: tt.status, Body: http.NoBody}, nil
			})}

			resp, err := tr.RoundTrip(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://idp.test/certs", nil))
			if err == nil {
				_ = resp.Body.Close()
			}

			if cleared := !failing.Load(); cleared != tt.wantClear {
				t.Errorf("flag cleared = %v, want %v", cleared, tt.wantClear)
			}
		})
	}
}
