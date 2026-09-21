package fxmod

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/telemetry"
)

// stopOrder records the OnStop hooks Fx ran, by the constructor that appended
// each one.
//
// An fxevent.Logger rather than a log line, because this is Fx's own account of
// its own lifecycle: CallerName is the function that appended the hook, so
// asserting on it is asserting on which component's hook ran when, which IS the
// shutdown ordering. A log line would be a restatement of the code that wrote
// it.
type stopOrder struct {
	mu      sync.Mutex
	stopped []string
}

func (s *stopOrder) logger() fxevent.Logger { return s }

func (s *stopOrder) LogEvent(event fxevent.Event) {
	stopped, ok := event.(*fxevent.OnStopExecuted)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = append(s.stopped, short(stopped.CallerName))
}

func (s *stopOrder) ran() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.stopped)
}

// at reports where a constructor's hook ran, and fails when none did.
func (s *stopOrder) at(t *testing.T, constructor string) int {
	t.Helper()
	if i := slices.Index(s.ran(), constructor); i >= 0 {
		return i
	}
	t.Fatalf("no OnStop hook appended by %s ran; the shutdown was %v", constructor, s.ran())
	return -1
}

// short is the constructor's own name out of the fully qualified one Fx reports.
func short(caller string) string {
	if dot := strings.LastIndex(caller, "."); dot >= 0 {
		return caller[dot+1:]
	}
	return caller
}

// telemetryEnvironment is a configuration with an exporting SDK and nothing
// else this package needs.
//
// The endpoint names a port nothing is listening on, deliberately: otlptracegrpc
// dials lazily, so building the exporter makes no connection and the process
// starts. That is the same property the "collector is unreachable" case rests
// on, and it is why this test needs no collector.
func telemetryEnvironment(overrides map[string]string) map[string]string {
	env := map[string]string{
		"DATABASE_URL":                "postgres://wagering:secret@127.0.0.1:5432/wagering",
		"AWS_REGION":                  "us-east-1",
		"OIDC_ISSUER":                 "http://127.0.0.1:8080/realms/wagering",
		"OIDC_AUDIENCE":               "wagering-api",
		"PUBLISHER_NAME":              "publisher-test",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4317",
		"TELEMETRY_SHUTDOWN_TIMEOUT":  "1s",
	}
	maps.Copy(env, overrides)
	return env
}

func loadedFrom(t *testing.T, env map[string]string) config.Config {
	t.Helper()
	cfg, err := config.Load(config.Static(env))
	if err != nil {
		t.Fatalf("load the configuration: %v", err)
	}
	return cfg
}

// afterwards is a module shaped like every other module in this package: a
// constructor that takes the logger and appends an OnStop hook.
//
// It stands in for [newPool], the three run* invokes and [serve] — everything
// whose work must be finished before telemetry is flushed. What it proves is
// the ordering property rather than any of those components: a hook appended by
// a module declared AFTER the telemetry module runs BEFORE the SDK's, so the
// SDK is still exporting while that component drains.
func afterwards() fx.Option {
	return fx.Module("afterwards",
		fx.Provide(newDrainingComponent),
		fx.Invoke(func(*drainingComponent) {}),
	)
}

// drainingComponent stands in for a pool, a server or a loop.
type drainingComponent struct{ drained bool }

// newDrainingComponent is named rather than anonymous so that Fx reports it by
// name in the event this test reads; an anonymous constructor is reported as
// "func1", which says nothing to whoever reads the failure.
func newDrainingComponent(lc fx.Lifecycle, _ *slog.Logger) *drainingComponent {
	component := &drainingComponent{}
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		component.drained = true
		return nil
	}})
	return component
}

// TestTelemetryIsFlushedAfterEverythingThatCouldStillEmit pins the seam this
// whole module is arranged around.
//
// Fx runs OnStop hooks in the reverse of the order they were appended, so
// "flushed last" is "appended first". The logger's hook is appended first
// unconditionally because fx.WithLogger forces its constructor during fx.New;
// the SDK's providers have no such forcing, so [installTelemetry] makes them
// exist before any other module's invoke can ask for anything.
//
// Without that invoke the first component to be built would append its hook
// ahead of the SDK's, and the SDK would be shut down while that component was
// still draining — the last spans of every shutdown, lost, silently, in a way
// no other test here would notice.
func TestTelemetryIsFlushedAfterEverythingThatCouldStillEmit(t *testing.T) {
	t.Parallel()
	ran := &stopOrder{}
	cfg := loadedFrom(t, telemetryEnvironment(nil))

	app := fx.New(
		fx.WithLogger(ran.logger),
		Telemetry(),
		Config(cfg),
		afterwards(),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("build the graph: %v", err)
	}
	startAndStop(t, app)

	drained := ran.at(t, "newDrainingComponent")
	traces := ran.at(t, "newTracerProvider")
	metrics := ran.at(t, "newMeterProvider")
	flushed := ran.at(t, "newLogger")

	if drained > traces || drained > metrics {
		t.Errorf("the SDK was shut down before a component had finished draining: %v", ran.ran())
	}
	if traces > flushed || metrics > flushed {
		t.Errorf("the last line was written before the SDK was flushed: %v", ran.ran())
	}
}

// TestNothingIsExportedWhenThereIsNowhereToExportTo pins how telemetry is
// switched off, and that switching it off leaves nothing behind.
//
// Two ways, because they are two different sentences for an operator: switched
// off deliberately, and never told where. Both build no exporter, no SDK
// provider and no shutdown hook — so a process with telemetry off makes no
// network call and has one fewer thing that can fail on its way down.
func TestNothingIsExportedWhenThereIsNowhereToExportTo(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		overrides map[string]string
	}{
		{
			name:      "switched off",
			overrides: map[string]string{"OTEL_SDK_DISABLED": "true"},
		},
		{
			name:      "no collector configured",
			overrides: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": ""},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ran := &stopOrder{}
			cfg := loadedFrom(t, telemetryEnvironment(c.overrides))
			if cfg.Telemetry.Exporting() {
				t.Fatalf("%v still reports somewhere to export to", c.overrides)
			}

			app := fx.New(fx.WithLogger(ran.logger), Telemetry(), Config(cfg))
			if err := app.Err(); err != nil {
				t.Fatalf("build the graph: %v", err)
			}
			startAndStop(t, app)

			for _, constructor := range []string{"newTracerProvider", "newMeterProvider"} {
				if slices.Contains(ran.ran(), constructor) {
					t.Errorf("%s appended a shutdown hook with nothing to shut down: %v",
						constructor, ran.ran())
				}
			}
			if !slices.Contains(ran.ran(), "newLogger") {
				t.Errorf("the last line was never written: %v", ran.ran())
			}
		})
	}
}

// TestAProcessStartsAndStopsWithACollectorThatIsNotAnswering pins the
// requirement that telemetry may never be the reason a deployment fails.
//
// The endpoint names a port nothing is listening on. otlptracegrpc dials
// lazily, so nothing here waits for a connection; the exports fail in the
// background and are reported through [otelErrors] as warnings. What must hold
// is that the graph builds, starts, stops within the telemetry budget, and
// reports no error from any hook.
func TestAProcessStartsAndStopsWithACollectorThatIsNotAnswering(t *testing.T) {
	t.Parallel()
	ran := &stopOrder{}
	cfg := loadedFrom(t, telemetryEnvironment(map[string]string{
		// A port in the ephemeral range nothing in this suite binds.
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1",
	}))

	app := fx.New(fx.WithLogger(ran.logger), Telemetry(), Config(cfg))
	if err := app.Err(); err != nil {
		t.Fatalf("a collector that is not answering stopped the graph building: %v", err)
	}

	started := time.Now()
	startAndStop(t, app)
	// The budget is a second and there are two providers to flush, so four is a
	// generous ceiling that still fails if a Shutdown were waiting on the
	// connection rather than on its own deadline.
	if took := time.Since(started); took > 4*time.Second {
		t.Errorf("starting and stopping took %s against a collector that is not answering", took)
	}
	if !slices.Contains(ran.ran(), "newLogger") {
		t.Errorf("the last line was never written: %v", ran.ran())
	}
}

// startAndStop runs the whole lifecycle and fails on anything either half
// reported.
func startAndStop(t *testing.T, app *fx.App) {
	t.Helper()
	startCtx, cancelStart := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelStart()
	if err := app.Start(startCtx); err != nil {
		t.Fatalf("start: %v", err)
	}
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelStop()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// TestTheDisabledTelemetryIsWhatAComponentGivenNoneUses is the composition
// root's half of the contract internal/telemetry states.
func TestTheDisabledTelemetryIsWhatAComponentGivenNoneUses(t *testing.T) {
	t.Parallel()
	if telemetry.Or(nil) != telemetry.Disabled() {
		t.Error("a component given no telemetry does not get the disabled one")
	}
}
