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
)

// ServerConfig is everything the server is built from.
//
// Every duration is required and must be positive. A timeout left at zero in
// net/http means no timeout at all, which is the one setting that turns a slow
// or hostile client into a connection this process holds until it is restarted
// — so there is no default here to fall back to, and a server that was not told
// is not built.
type ServerConfig struct {
	// Addr is the address to listen on, in net.Listen's form. ":0" binds an
	// arbitrary free port, which is what a test wants and [Server.Addr] reports.
	Addr string
	// Handler answers every request. Usually an [API].
	Handler http.Handler

	// ReadTimeout bounds reading a whole request, headers and body.
	ReadTimeout time.Duration
	// ReadHeaderTimeout bounds reading the headers alone, so a client that
	// opens a connection and sends nothing is not holding one.
	ReadHeaderTimeout time.Duration
	// WriteTimeout bounds writing a response.
	WriteTimeout time.Duration
	// IdleTimeout bounds how long a kept-alive connection may sit unused.
	IdleTimeout time.Duration
	// ShutdownTimeout is how long [Server.Stop] waits for requests in flight.
	ShutdownTimeout time.Duration

	// MaxHeaderBytes bounds the headers of one request.
	MaxHeaderBytes int
	// Logger is where the server reports what it did.
	Logger *slog.Logger
}

// Server carries the API over HTTP.
//
// Start and Stop are separate calls that do not block, so a composition root
// can drive them from a lifecycle hook without owning a goroutine of its own.
// Nothing here imports a dependency-injection framework.
type Server struct {
	server   *http.Server
	shutdown time.Duration
	logger   *slog.Logger

	mu       sync.Mutex
	listener net.Listener
	// served is closed when Serve has returned, so Stop can report a shutdown
	// that finished rather than one that was merely asked for.
	served chan struct{}
}

// NewServer wires the server.
func NewServer(cfg ServerConfig) (*Server, error) {
	switch {
	case cfg.Handler == nil:
		return nil, errors.New("httpapi: the server needs a handler")
	case cfg.Logger == nil:
		return nil, errors.New("httpapi: the server needs a logger")
	case cfg.Addr == "":
		return nil, errors.New("httpapi: the server needs an address to listen on")
	case cfg.MaxHeaderBytes <= 0:
		return nil, errors.New("httpapi: the server needs a positive header size limit")
	}
	for _, bound := range []struct {
		name string
		of   time.Duration
	}{
		{"read timeout", cfg.ReadTimeout},
		{"read header timeout", cfg.ReadHeaderTimeout},
		{"write timeout", cfg.WriteTimeout},
		{"idle timeout", cfg.IdleTimeout},
		{"shutdown timeout", cfg.ShutdownTimeout},
	} {
		if bound.of <= 0 {
			return nil, fmt.Errorf("httpapi: the server needs a positive %s, got %s",
				bound.name, bound.of)
		}
	}

	return &Server{
		server: &http.Server{
			Addr:              cfg.Addr,
			Handler:           cfg.Handler,
			ReadTimeout:       cfg.ReadTimeout,
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
			MaxHeaderBytes:    cfg.MaxHeaderBytes,
			ErrorLog:          slog.NewLogLogger(cfg.Logger.Handler(), slog.LevelWarn),
		},
		shutdown: cfg.ShutdownTimeout,
		logger:   cfg.Logger,
	}, nil
}

// Start binds the address and begins serving.
//
// Binding happens here rather than in the goroutine below, so that a port
// already in use is a start-up failure reported to whoever is starting the
// process. A Serve that failed to bind inside a goroutine would leave the
// process apparently up and answering nothing.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return errors.New("httpapi: the server is already listening")
	}

	var config net.ListenConfig
	listener, err := config.Listen(ctx, "tcp", s.server.Addr)
	if err != nil {
		return fmt.Errorf("httpapi: listen on %s: %w", s.server.Addr, err)
	}
	s.listener = listener
	s.served = make(chan struct{})

	served := s.served
	go func() {
		defer close(served)
		if err := s.server.Serve(listener); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			s.logger.ErrorContext(context.WithoutCancel(ctx), "the server stopped serving",
				slog.String("error", err.Error()))
		}
	}()
	s.logger.InfoContext(ctx, "serving", slog.String("address", listener.Addr().String()))
	return nil
}

// Addr reports the address actually bound, which is what a test that asked for
// ":0" needs and what a log line should say rather than the address requested.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Stop stops accepting and drains what is in flight.
//
// It reports whether the deadline was hit: an error matching
// [context.DeadlineExceeded] means requests were still running when the budget
// ran out and their connections were dropped, which is a fact an operator
// reading a deployment's logs needs and a silent success would hide.
//
// The deadline is this server's own rather than the caller's alone, so a
// lifecycle hook that passes a context with no deadline still drains within a
// bound.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	served := s.served
	s.mu.Unlock()
	if served == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.shutdown)
	defer cancel()

	err := s.server.Shutdown(ctx)
	// Serve returns as soon as the listeners are closed, which Shutdown does
	// first, so this waits on something that has already been set in motion
	// whether the drain finished or not.
	<-served
	if err != nil {
		return fmt.Errorf("httpapi: the shutdown deadline passed with requests in flight: %w", err)
	}
	s.logger.InfoContext(ctx, "stopped serving")
	return nil
}
