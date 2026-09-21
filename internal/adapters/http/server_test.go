package httpapi

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// bounds is a configuration every value of which is set, so a test that wants
// one thing wrong changes one thing.
func bounds(handler http.Handler) ServerConfig {
	return ServerConfig{
		Addr:              "127.0.0.1:0",
		Handler:           handler,
		ReadTimeout:       5 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
		ShutdownTimeout:   5 * time.Second,
		MaxHeaderBytes:    8192,
		Logger:            discard(),
	}
}

func TestNewServerRefusesAnUnboundedServer(t *testing.T) {
	t.Parallel()
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

	cases := []struct {
		name   string
		damage func(cfg *ServerConfig)
	}{
		{"no handler", func(cfg *ServerConfig) { cfg.Handler = nil }},
		{"no logger", func(cfg *ServerConfig) { cfg.Logger = nil }},
		{"no address", func(cfg *ServerConfig) { cfg.Addr = "" }},
		{"no header size limit", func(cfg *ServerConfig) { cfg.MaxHeaderBytes = 0 }},
		{"no read timeout", func(cfg *ServerConfig) { cfg.ReadTimeout = 0 }},
		{"no read header timeout", func(cfg *ServerConfig) { cfg.ReadHeaderTimeout = 0 }},
		{"no write timeout", func(cfg *ServerConfig) { cfg.WriteTimeout = 0 }},
		{"no idle timeout", func(cfg *ServerConfig) { cfg.IdleTimeout = 0 }},
		{"no shutdown timeout", func(cfg *ServerConfig) { cfg.ShutdownTimeout = 0 }},
		{"a negative timeout", func(cfg *ServerConfig) { cfg.WriteTimeout = -time.Second }},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			cfg := bounds(handler)
			c.damage(&cfg)

			// A timeout left at zero in net/http means no timeout at all, which
			// is the one setting that lets a slow client hold a connection
			// until this process is restarted. There is no default to fall back
			// to, so a server that was not told is not built.
			if _, err := NewServer(cfg); err == nil {
				t.Fatal("a server was built without it")
			}
		})
	}
}

func TestEveryBoundReachesTheServer(t *testing.T) {
	t.Parallel()
	cfg := bounds(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("building the server: %v", err)
	}

	cases := []struct {
		name      string
		got, want any
	}{
		{"read timeout", server.server.ReadTimeout, cfg.ReadTimeout},
		{"read header timeout", server.server.ReadHeaderTimeout, cfg.ReadHeaderTimeout},
		{"write timeout", server.server.WriteTimeout, cfg.WriteTimeout},
		{"idle timeout", server.server.IdleTimeout, cfg.IdleTimeout},
		{"header size limit", server.server.MaxHeaderBytes, cfg.MaxHeaderBytes},
		{"shutdown timeout", server.shutdown, cfg.ShutdownTimeout},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// The bound above, actually bounding something: a client that opens a
// connection and never finishes its headers is let go of.
func TestAConnectionThatSendsNoHeadersIsLetGoOf(t *testing.T) {
	t.Parallel()
	cfg := bounds(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	// Far apart, so that it is the header bound being tested and not the other.
	cfg.ReadHeaderTimeout = 100 * time.Millisecond
	cfg.ReadTimeout = 10 * time.Second
	server := started(t, cfg)

	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", server.Addr())
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := io.WriteString(conn, "GET /health/live HTTP/1.1\r\nHost: x\r\n"); err != nil {
		t.Fatalf("writing a partial request: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("setting a deadline: %v", err)
	}

	begun := time.Now()
	_, _ = io.ReadAll(conn)
	if elapsed := time.Since(begun); elapsed > 2*time.Second {
		t.Errorf("the connection was held for %s, want the header bound to end it", elapsed)
	}
}

// started builds a server, starts it, and stops it when the test ends.
func started(t *testing.T, cfg ServerConfig) *Server {
	t.Helper()
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("building the server: %v", err)
	}
	if err := server.Start(t.Context()); err != nil {
		t.Fatalf("starting the server: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop(context.WithoutCancel(t.Context())) })
	return server
}

func TestStartBindsAndReportsTheAddress(t *testing.T) {
	t.Parallel()
	server := started(t, bounds(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})))

	if server.Addr() == "" {
		t.Fatal("a started server reported no address")
	}
	// Binding happens in Start rather than in the goroutine, so a port already
	// in use is a start-up failure and not a process that is up and answering
	// nothing.
	second, err := NewServer(bounds(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})))
	if err != nil {
		t.Fatalf("building the second server: %v", err)
	}
	second.server.Addr = server.Addr()
	if err := second.Start(t.Context()); err == nil {
		_ = second.Stop(t.Context())
		t.Fatal("a second server bound an address already in use")
	}

	response, err := get(t, "http://"+server.Addr()+"/anything")
	if err != nil {
		t.Fatalf("calling the server: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusTeapot {
		t.Errorf("status = %d, want the handler's", response.StatusCode)
	}
}

func get(t *testing.T, url string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func TestStopDrainsARequestInFlight(t *testing.T) {
	t.Parallel()
	running := make(chan struct{})
	cfg := bounds(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(running)
		// Long enough that the shutdown below is entered while this is still
		// going, and well inside the shutdown budget.
		time.Sleep(200 * time.Millisecond)
		_, _ = io.WriteString(w, "drained")
	}))
	server := started(t, cfg)

	type answer struct {
		body string
		err  error
	}
	answers := make(chan answer, 1)
	go func() {
		response, err := get(t, "http://"+server.Addr()+"/slow")
		if err != nil {
			answers <- answer{err: err}
			return
		}
		defer func() { _ = response.Body.Close() }()
		body, err := io.ReadAll(response.Body)
		answers <- answer{body: string(body), err: err}
	}()

	<-running
	if err := server.Stop(context.WithoutCancel(t.Context())); err != nil {
		t.Fatalf("stopping with a request in flight: %v", err)
	}

	got := <-answers
	if got.err != nil {
		t.Fatalf("the request in flight was dropped: %v", got.err)
	}
	if got.body != "drained" {
		t.Errorf("body = %q, want the whole answer", got.body)
	}
}

func TestStopReportsADeadlineItCouldNotDrainWithin(t *testing.T) {
	t.Parallel()
	running := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	cfg := bounds(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(running)
		<-release
		_, _ = io.WriteString(w, "eventually")
	}))
	cfg.ShutdownTimeout = 100 * time.Millisecond
	server := started(t, cfg)

	go func() {
		response, err := get(t, "http://"+server.Addr()+"/stuck")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
	}()
	<-running

	err := server.Stop(context.WithoutCancel(t.Context()))
	if err == nil {
		t.Fatal("a shutdown that dropped a request in flight reported success")
	}
	// A deployment's logs have to be able to say the difference between a
	// drain that finished and one that ran out of time.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want it to name the deadline", err)
	}
}

func TestStoppingAServerThatNeverStartedIsNothing(t *testing.T) {
	t.Parallel()
	server, err := NewServer(bounds(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})))
	if err != nil {
		t.Fatalf("building the server: %v", err)
	}
	if err := server.Stop(t.Context()); err != nil {
		t.Errorf("stopping a server that never started: %v", err)
	}
	if server.Addr() != "" {
		t.Error("a server that never started reported an address")
	}
}

func TestStartingTwiceIsRefused(t *testing.T) {
	t.Parallel()
	server := started(t, bounds(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})))

	if err := server.Start(t.Context()); err == nil {
		t.Fatal("a server listening twice was allowed")
	}
}
