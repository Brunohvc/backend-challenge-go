package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Brunohvc/backend-challenge-go/internal/platform/config"
)

var errAlreadyStarted = errors.New("httpapi: server already started")

// Server is the public HTTP server.
type Server struct {
	addr            string
	shutdownTimeout time.Duration
	srv             *http.Server

	done     chan struct{}
	serveErr error

	mu       sync.Mutex
	listener net.Listener
}

// NewServer returns a server for h with the limits of cfg.
func NewServer(cfg config.HTTPConfig, h http.Handler, log *slog.Logger) *Server {
	return &Server{
		addr:            cfg.Addr,
		shutdownTimeout: cfg.ShutdownTimeout,
		done:            make(chan struct{}),
		srv: &http.Server{
			Handler:           withDeadline(h, cfg.RequestTimeout),
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			ReadTimeout:       cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
			MaxHeaderBytes:    cfg.MaxHeaderBytes,
			ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		},
	}
}

func withDeadline(h http.Handler, d time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Start binds the address and serves in the background.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return errAlreadyStarted
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("httpapi: listen on %s: %w", s.addr, err)
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

// Done returns a channel closed when the server stops serving.
func (s *Server) Done() <-chan struct{} {
	return s.done
}

// Err returns the error that ended serving, such as a listener that failed.
func (s *Server) Err() error {
	select {
	case <-s.done:
		return s.serveErr
	default:
		return nil
	}
}

// Addr returns the address the server listens on.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Stop stops accepting connections and waits for in-flight requests within the shutdown timeout.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	started := s.listener != nil
	s.mu.Unlock()
	if !started {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.shutdownTimeout)
	defer cancel()
	if err := s.srv.Shutdown(ctx); err != nil {
		_ = s.srv.Close()
		return fmt.Errorf("httpapi: shut down: %w", err)
	}
	select {
	case <-s.done:
	default:
		select {
		case <-s.done:
		case <-ctx.Done():
			return fmt.Errorf("httpapi: wait for server: %w", ctx.Err())
		}
	}
	return nil
}
