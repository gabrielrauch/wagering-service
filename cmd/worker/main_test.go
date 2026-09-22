package main

import (
	"strconv"
	"strings"
	"testing"

	"go.uber.org/fx"

	"github.com/gabrielrauch/wagering-service/internal/adapters/oidc"
	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/fxmod"
)

// environment is the smallest one this binary starts from.
//
// Written out rather than read from .env.example for the reason cmd/api's copy
// gives: this file asserts a property of the graph, and internal/config has the
// test that keeps the example honest.
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

// switches returns the environment with the three loops set as given.
func switches(consumer, publisher, reference bool) map[string]string {
	env := environment()
	env["CONSUMER_ENABLED"] = strconv.FormatBool(consumer)
	env["PUBLISHER_ENABLED"] = strconv.FormatBool(publisher)
	env["REFERENCE_WORKER_ENABLED"] = strconv.FormatBool(reference)
	return env
}

// TestEveryCombinationOfLoopsIsSatisfiable is the cheap half of proving this
// binary is wired.
//
// Every combination, not just the default one, because this graph is not one
// graph: a loop that is switched off is left out of the options entirely, which
// takes its dependencies out with it. The combination that would break first is
// the publisher alone — it is the only loop that needs no application service,
// so it is the one that would find a provider the others were quietly supplying.
//
// fx.ValidateApp resolves the graph without running a constructor, so none of
// this needs a container. The start-and-stop proof is in internal/fxmod, behind
// the integration tag.
func TestEveryCombinationOfLoopsIsSatisfiable(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                           string
		consumer, publisher, reference bool
	}{
		{"all three", true, true, true},
		{"consumer alone", true, false, false},
		{"publisher alone", false, true, false},
		{"reference worker alone", false, false, true},
		{"consumer and publisher", true, true, false},
		{"consumer and reference worker", true, false, true},
		{"publisher and reference worker", false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := load(t, switches(tc.consumer, tc.publisher, tc.reference))
			if err := fx.ValidateApp(fxmod.Worker(cfg)); err != nil {
				t.Fatalf("the worker's graph is not satisfiable with %s: %v", tc.name, err)
			}
		})
	}
}

// TestNoLoopsIsRefused covers the configuration that looks healthiest and is
// always wrong.
//
// A worker with all three switched off starts, holds a pool open, reports
// itself up and consumes nothing, which is indistinguishable from a working
// deployment until somebody notices the queue depth.
func TestNoLoopsIsRefused(t *testing.T) {
	t.Parallel()

	cfg := load(t, switches(false, false, false))
	err := fx.ValidateApp(fxmod.Worker(cfg))
	if err == nil {
		t.Fatal("a worker with every loop switched off was accepted")
	}
	for _, name := range []string{
		"CONSUMER_ENABLED", "PUBLISHER_ENABLED", "REFERENCE_WORKER_ENABLED",
	} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("the refusal did not say how to switch a loop on (%s): %v", name, err)
		}
	}
}

// TestTheWorkerAuthenticatesNothing pins the other half of the split with
// cmd/api.
//
// Nothing a worker does is authenticated — a message on the inbound queue was
// authorised by whatever put it there — so an identity provider in this graph
// would be a start-up dependency on a service this binary never calls, and a
// Keycloak outage would stop the queues draining.
func TestTheWorkerAuthenticatesNothing(t *testing.T) {
	t.Parallel()

	cfg := load(t, environment())
	err := fx.ValidateApp(fxmod.Worker(cfg), fx.Invoke(func(*oidc.Authenticator) {}))
	if err == nil {
		t.Fatal("the worker's graph provides an authenticator, so it would fail to " +
			"start while the identity provider is down")
	}
	if !strings.Contains(err.Error(), "missing type") {
		t.Fatalf("the graph refused the authenticator for some other reason: %v", err)
	}
}
