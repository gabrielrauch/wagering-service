package fxmod

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/gabrielrauch/wagering-service/internal/config"
)

// minimalEnvironment is the smallest one either binary starts from.
func minimalEnvironment() map[string]string {
	return map[string]string{
		"DATABASE_URL":   "postgres://app:hunter2@127.0.0.1:1/wagering?sslmode=disable",
		"AWS_REGION":     "us-east-1",
		"OIDC_ISSUER":    "http://localhost:8080/realms/wagering",
		"OIDC_AUDIENCE":  "wagering-api",
		"PUBLISHER_NAME": "publisher-1",
	}
}

// TestRunTellsAWrongEnvironmentFromAFailedStart is why there are two non-zero
// exit codes rather than one.
//
// A supervisor that restarts a process which could not reach its database is
// doing the right thing; one that restarts a process whose environment is
// missing a variable is doing it four hundred times a minute. The codes are the
// only thing a supervisor can tell them apart by.
func TestRunTellsAWrongEnvironmentFromAFailedStart(t *testing.T) {
	t.Parallel()

	var reported strings.Builder
	code := Run(t.Context(), &reported, "api", config.Static(map[string]string{}), API)

	if code != ExitConfig {
		t.Fatalf("an unconfigurable environment exited %d, want %d", code, ExitConfig)
	}
	if !strings.Contains(reported.String(), "DATABASE_URL") {
		t.Errorf("the message did not name a variable that was missing: %q", reported.String())
	}
	if !strings.HasPrefix(reported.String(), "api: ") {
		t.Errorf("the message did not say which binary refused: %q", reported.String())
	}
}

// TestRunReportsAGraphThatWillNotBuild covers the other non-zero path.
//
// A worker with every loop switched off is refused while the graph is being
// built, before any constructor runs — which is why this test can assert on it
// without a database at all.
func TestRunReportsAGraphThatWillNotBuild(t *testing.T) {
	t.Parallel()

	env := minimalEnvironment()
	env["CONSUMER_ENABLED"] = "false"
	env["PUBLISHER_ENABLED"] = "false"
	env["REFERENCE_WORKER_ENABLED"] = "false"

	var reported strings.Builder
	code := Run(t.Context(), &reported, "worker", config.Static(env), Worker)

	if code != ExitFailed {
		t.Fatalf("a graph that will not build exited %d, want %d", code, ExitFailed)
	}
	if !strings.Contains(reported.String(), "CONSUMER_ENABLED") {
		t.Errorf("the message did not say how to switch a loop on: %q", reported.String())
	}
}

// TestRunNeverReportsThePassword is the start-up half of the rule internal/config
// keeps for its own messages.
//
// This is where a DSN is most likely to be printed by accident: the graph is
// built from it and every constructor that refuses one has it in reach.
func TestRunNeverReportsThePassword(t *testing.T) {
	t.Parallel()

	const password = "hunter2"
	env := minimalEnvironment()
	// Wrong in a way that fails while the graph is being built, with the DSN
	// present and valid — which is the shape that could carry it out.
	env["DATABASE_CONNECT_TIMEOUT"] = "50ms"

	var reported strings.Builder
	code := Run(t.Context(), &reported, "worker", config.Static(env), Worker)

	if code == ExitOK {
		t.Fatalf("a worker started against a database nobody can reach: %q", reported.String())
	}
	if strings.Contains(reported.String(), password) {
		t.Errorf("the message carried the password: %q", reported.String())
	}
}

// TestStopWorkerNamesTheLoop covers what an operator reads after a shutdown
// that left work behind.
//
// Every worker's Stop reports a drain that ran out of time, and two of them
// report it in nearly the same words, so the report is useless without the name
// of the loop that produced it. The cause is kept in the chain because a caller
// that wants to know WHAT did not finish — which receivers, how many messages
// were released — needs the error the worker built, not a rendering of it.
func TestStopWorkerNamesTheLoop(t *testing.T) {
	t.Parallel()

	cause := errors.New("the drain deadline of 20s passed with 2 receivers still working")
	stop := stopWorker("consumer", func(context.Context) error { return cause })

	err := stop(t.Context())
	if err == nil {
		t.Fatal("a drain that ran out of time was reported as a clean stop")
	}
	if !strings.Contains(err.Error(), "consumer") {
		t.Errorf("the report did not name the loop: %v", err)
	}
	if !errors.Is(err, cause) {
		t.Errorf("the report lost the worker's own error: %v", err)
	}
}

// TestStopWorkerSaysNothingAboutACleanStop keeps the case above from being
// bought with a hook that always fails.
func TestStopWorkerSaysNothingAboutACleanStop(t *testing.T) {
	t.Parallel()

	stop := stopWorker("publisher", func(context.Context) error { return nil })
	if err := stop(t.Context()); err != nil {
		t.Errorf("a clean stop was reported as a failure: %v", err)
	}
}

// TestTheShutdownBudgetSurvivesTheSignalThatStartedIt.
//
// The context a SIGTERM cancelled is the one that woke the shutdown, so a
// budget derived from it is already spent: every drain would be handed a
// deadline in the past, the server would cut off the requests in flight and
// each worker would abandon its work rather than giving it back. This is one
// line in [Run] and it is the line that decides whether a deployment is
// graceful.
func TestTheShutdownBudgetSurvivesTheSignalThatStartedIt(t *testing.T) {
	t.Parallel()

	type key struct{}
	signalled, cancel := context.WithCancel(context.WithValue(
		context.Background(), key{}, "correlation"))
	cancel()

	stop, release := stopContext(signalled, 30*time.Second)
	defer release()

	if err := stop.Err(); err != nil {
		t.Fatalf("the shutdown began with no budget at all: %v", err)
	}
	deadline, ok := stop.Deadline()
	if !ok {
		t.Fatal("the shutdown has no deadline, so a drain that hangs holds it open")
	}
	if left := time.Until(deadline); left < 25*time.Second {
		t.Errorf("the shutdown has %s left of a 30s budget", left)
	}
	// The values are kept: a correlation established at start-up belongs on the
	// lines the shutdown writes.
	if stop.Value(key{}) != "correlation" {
		t.Error("the shutdown lost the context's values along with its cancellation")
	}
}

// TestRunTellsACleanShutdownFromOneThatLeftWorkBehind is why there is a third
// non-zero exit code.
//
// A drain that ran out of time and a start that failed want the same thing from
// a supervisor — restart — and different things from an operator: the second is
// a deployment that never came up, the first is messages abandoned mid-flight
// and outbox claims left held, which is a latency incident somebody will
// otherwise meet as a mystery. Collapsed into one code they are
// indistinguishable from outside the process.
//
// The graph is this test's own, because what is under test is [Run]'s reading
// of what Fx gives it and not any particular component's shutdown.
func TestRunTellsACleanShutdownFromOneThatLeftWorkBehind(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		stop error
		want int
	}{
		{"a drain that finished", nil, ExitOK},
		{"a drain that did not", errors.New("2 receivers still working"), ExitUnclean},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Ended through the Shutdowner rather than by cancelling the
			// context, because a cancellation that lands while Fx is still
			// running OnStart hooks fails the START — which is a different
			// exit code and not the one under test. The signal path is proved
			// end to end by TestRunCarriesTheAPIFromTheEnvironmentToACleanExit.
			graph := func(config.Config) fx.Option {
				return fx.Options(
					fx.StopTimeout(5*time.Second),
					fx.Invoke(func(lc fx.Lifecycle, shutdowner fx.Shutdowner) {
						lc.Append(fx.Hook{
							OnStart: func(context.Context) error {
								go func() { _ = shutdowner.Shutdown() }()
								return nil
							},
							OnStop: func(context.Context) error { return tc.stop },
						})
					}),
				)
			}

			var reported strings.Builder
			code := Run(context.Background(), &reported, "worker",
				config.Static(minimalEnvironment()), graph)
			if code != tc.want {
				t.Fatalf("exited %d, want %d: %s", code, tc.want, reported.String())
			}
		})
	}
}

// TestAComponentCanEndTheProcess is the other way out of [Run].
//
// Without it a graph that has stopped doing its job has no way to say so, and
// the only failure mode left is the worst one this system has: a process that
// is up, looks healthy and is doing nothing. Fx puts an fx.Shutdowner in every
// graph here; this is what makes asking it for anything.
func TestAComponentCanEndTheProcess(t *testing.T) {
	t.Parallel()

	// Never cancelled. The process must end on the component's word alone.
	graph := func(config.Config) fx.Option {
		return fx.Options(
			fx.StopTimeout(5*time.Second),
			fx.Invoke(func(lc fx.Lifecycle, shutdowner fx.Shutdowner) {
				lc.Append(fx.Hook{OnStart: func(context.Context) error {
					go func() { _ = shutdowner.Shutdown() }()
					return nil
				}})
			}),
		)
	}

	done := make(chan int, 1)
	go func() {
		var reported strings.Builder
		done <- Run(context.Background(), &reported, "worker",
			config.Static(minimalEnvironment()), graph)
	}()

	select {
	case code := <-done:
		if code != ExitOK {
			t.Fatalf("a component asked the process to stop and it exited %d, want %d",
				code, ExitOK)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("a component asked the process to stop and nothing happened; only a " +
			"signal can end it")
	}
}
