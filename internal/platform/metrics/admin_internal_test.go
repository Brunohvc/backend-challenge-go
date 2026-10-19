package metrics

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func startedAdmin(t *testing.T) *AdminServer {
	t.Helper()
	srv := NewAdminServer("127.0.0.1:0", prometheus.NewRegistry())
	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return srv
}

func TestAdmin_ReportsServingEndedOnItsOwn(t *testing.T) {
	srv := startedAdmin(t)
	t.Cleanup(func() { _ = srv.Stop(context.WithoutCancel(t.Context())) })
	select {
	case <-srv.Done():
		t.Fatal("Done is closed while the server serves")
	default:
	}
	if err := srv.Err(); err != nil {
		t.Fatalf("Err() while serving = %v, want nil", err)
	}

	if err := srv.listener.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-srv.Done():
	case <-time.After(time.Second):
		t.Fatal("Done is still open a second after the listener failed")
	}
	if err := srv.Err(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("Err() = %v, want net.ErrClosed", err)
	}
	if err := srv.Stop(t.Context()); err != nil {
		t.Errorf("Stop() after serving ended = %v, want nil", err)
	}
}

func TestAdmin_StopEndsServingWithoutError(t *testing.T) {
	srv := startedAdmin(t)

	if err := srv.Stop(t.Context()); err != nil {
		t.Fatalf("Stop() = %v", err)
	}

	select {
	case <-srv.Done():
	default:
		t.Fatal("Done is still open after Stop returned")
	}
	if err := srv.Err(); err != nil {
		t.Errorf("Err() after Stop = %v, want nil", err)
	}
}

func TestAdmin_StartsOnce(t *testing.T) {
	srv := startedAdmin(t)
	t.Cleanup(func() { _ = srv.Stop(context.WithoutCancel(t.Context())) })

	if err := srv.Start(t.Context()); err == nil {
		t.Error("second Start succeeded, want an error")
	}
}
