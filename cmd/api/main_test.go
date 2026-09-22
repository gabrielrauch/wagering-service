package main

import (
	"strings"
	"testing"

	"go.uber.org/fx"

	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/fxmod"
	"github.com/gabrielrauch/wagering-service/internal/workers"
)

// environment is the smallest one this binary starts from.
//
// It is written out rather than read from .env.example, because what this file
// is asserting is a property of the graph and not of that file — internal/config
// has the test that keeps the example honest, and coupling this one to it would
// make a comment moved in that file break a test about dependency injection.
func environment() map[string]string {
	return map[string]string{
		"DATABASE_URL":   "postgres://app:secret@db:5432/wagering?sslmode=disable",
		"AWS_REGION":     "us-east-1",
		"OIDC_ISSUER":    "http://localhost:8080/realms/wagering",
		"OIDC_AUDIENCE":  "wagering-api",
		"PUBLISHER_NAME": "publisher-1",
	}
}

func load(t *testing.T, env map[string]string) config.Config {
	t.Helper()
	cfg, err := config.Load(config.Static(env))
	if err != nil {
		t.Fatalf("load the configuration: %v", err)
	}
	return cfg
}

// TestGraphIsSatisfiable is the cheap half of proving this binary is wired.
//
// fx.ValidateApp builds the dependency graph and resolves every invoke without
// running a constructor, so it catches the two failures that are otherwise only
// found by starting the process against real infrastructure: a dependency
// nothing provides, and a type two providers both claim. Neither needs a
// container, so this runs on every `go test ./...` with no build tag —
// the start-and-stop proof is in internal/fxmod, behind one.
func TestGraphIsSatisfiable(t *testing.T) {
	t.Parallel()

	if err := fx.ValidateApp(fxmod.API(load(t, environment()))); err != nil {
		t.Fatalf("the API's graph is not satisfiable: %v", err)
	}
}

// TestGraphDoesNotRunTheLoops pins what this binary is for.
//
// A worker's loop reaching the API graph would be a wallet's messages consumed
// by every API replica as well as by every worker one, which the inbox absorbs
// and the latency does not. The assertion is that the graph cannot satisfy a
// request for one: fx.ValidateApp fails on a missing provider, so an invoke
// that asks for a consumer proves nothing provides one.
func TestGraphDoesNotRunTheLoops(t *testing.T) {
	t.Parallel()

	cfg := load(t, environment())
	for _, tc := range []struct {
		name string
		want any
	}{
		{"consumer", func(*workers.Consumer) {}},
		{"publisher", func(*workers.Publisher) {}},
		{"reference worker", func(*workers.ReferenceWorker) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := fx.ValidateApp(fxmod.API(cfg), fx.Invoke(tc.want))
			if err == nil {
				t.Fatalf("the API's graph provides a %s", tc.name)
			}
			if !strings.Contains(err.Error(), "missing type") {
				t.Fatalf("the graph refused the %s for some other reason: %v", tc.name, err)
			}
		})
	}
}
