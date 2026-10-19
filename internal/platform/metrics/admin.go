package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	adminReadHeaderTimeout = 5 * time.Second
	adminWriteTimeout      = 30 * time.Second
	adminIdleTimeout       = 60 * time.Second
)

var errAlreadyStarted = errors.New("metrics: admin server already started")

// AdminServer serves the metrics of a registry on /metrics, apart from the public API.
type AdminServer struct {
	addr string
	srv  *http.Server

	done     chan struct{}
	serveErr error

	mu       sync.Mutex
	listener net.Listener
}

// NewAdminServer returns a server for the metrics of reg on addr.
func NewAdminServer(addr string, reg *prometheus.Registry) *AdminServer {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	return &AdminServer{
		addr: addr,
		done: make(chan struct{}),
		srv: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: adminReadHeaderTimeout,
			WriteTimeout:      adminWriteTimeout,
			IdleTimeout:       adminIdleTimeout,
		},
	}
}

// Start binds the address and serves in the background.
func (s *AdminServer) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return errAlreadyStarted
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("metrics: listen on %s: %w", s.addr, err)
	}
	s.listener = ln

	go func() {
		err := s.srv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		s.serveErr = err
		close(s.done)
	}()
	return nil
}

// Done returns a channel that is closed when the server stops serving.
func (s *AdminServer) Done() <-chan struct{} {
	return s.done
}

// Err returns the error that ended serving, nil while serving or after a requested stop.
func (s *AdminServer) Err() error {
	select {
	case <-s.done:
		return s.serveErr
	default:
		return nil
	}
}

// Addr returns the address the server listens on, empty before Start.
func (s *AdminServer) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Stop shuts the server down gracefully within ctx.
func (s *AdminServer) Stop(ctx context.Context) error {
	s.mu.Lock()
	started := s.listener != nil
	s.mu.Unlock()
	if !started {
		return nil
	}
	if err := s.srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("metrics: shut down admin server: %w", err)
	}
	select {
	case <-s.done:
	default:
		select {
		case <-s.done:
		case <-ctx.Done():
			return fmt.Errorf("metrics: wait for admin server: %w", ctx.Err())
		}
	}
	return nil
}
