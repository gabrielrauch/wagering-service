package fxmod

import (
	"maps"
	"testing"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/config"
	"github.com/gabrielrauch/wagering-service/internal/workers"
)

// TestTheConsumerIsNamedTheSameOnEveryReplicaAndThePublisherIsNot is the
// asymmetry this composition root exists to get right.
//
// The two names look alike and the rules are opposite. A consumer name is what
// makes the inbox's one row per consumer per message turn a redelivery into a
// replay, so two replicas must be one consumer; a publisher name lands in
// outbox.claimed_by and scopes a reschedule to the process holding the row, so
// two replicas must be two publishers. Getting either the wrong way round is
// silent: the first applies a message twice, the second has two publishers
// putting back each other's claims, and neither shows up as an error anywhere.
//
// Asserted through the real loader, on two environments that differ only in
// what a container runtime makes differ.
func TestTheConsumerIsNamedTheSameOnEveryReplicaAndThePublisherIsNot(t *testing.T) {
	t.Parallel()

	replica := func(host string) config.Config {
		t.Helper()
		env := minimalEnvironment()
		delete(env, "PUBLISHER_NAME")
		env["HOSTNAME"] = host
		cfg, err := config.Load(config.Static(env))
		if err != nil {
			t.Fatalf("load for %s: %v", host, err)
		}
		return cfg
	}
	first, second := replica("pod-1"), replica("pod-2")

	if got, want := consumerSettings(first.Consumer, first.SQS).Name,
		consumerSettings(second.Consumer, second.SQS).Name; got != want {
		t.Errorf("two replicas are consumers %q and %q; a message redelivered to the "+
			"other one would be applied a second time", got, want)
	}
	if got, other := publisherSettings(first.Publisher).Name,
		publisherSettings(second.Publisher).Name; got == other {
		t.Errorf("two replicas are both publisher %q; each would reschedule the other's "+
			"outbox claims", got)
	}
	if got, want := referenceSettings(first.Reference).Name,
		referenceSettings(second.Reference).Name; got != want {
		t.Errorf("two replicas name the reference worker %q and %q in the audit trail",
			got, want)
	}
}

// TestTheRedrivePolicyReachesTheConsumer.
//
// Zero means the consumer says nothing about which delivery is the last one
// before the dead-letter queue, and that line is the one an operator most wants
// when a message is about to be dropped. It comes from the SQS section rather
// than the consumer's, because it is a property of the queue, and a wiring that
// read it from the wrong section would pass every other test in this package.
func TestTheRedrivePolicyReachesTheConsumer(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(config.Static(minimalEnvironment()))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// The number deploy/localstack/01-queues.sh provisions. A consumer told a
	// different one names the wrong delivery as the last.
	const provisioned = 5

	if got := cfg.SQS.MaxReceiveCount; got != provisioned {
		t.Fatalf("the configured redrive policy is %d, want %d", got, provisioned)
	}
	if got := consumerSettings(cfg.Consumer, cfg.SQS).MaxReceiveCount; got != provisioned {
		t.Errorf("the consumer was wired with a redrive policy of %d, want %d",
			got, provisioned)
	}
}

// TestEverySettingReachesTheWorkerItIsFor.
//
// Each of these is a number an operator sets and nothing reports back. A field
// dropped on the way through is a service running on the worker package's
// default while its environment says otherwise, which is invisible until
// somebody tunes one and nothing changes.
func TestEverySettingReachesTheWorkerItIsFor(t *testing.T) {
	t.Parallel()

	env := minimalEnvironment()
	maps.Copy(env, map[string]string{
		"CONSUMER_NAME":                    "some-consumer",
		"CONSUMER_CONCURRENCY":             "7",
		"CONSUMER_DRAIN_TIMEOUT":           "11s",
		"CONSUMER_BACKOFF_INITIAL":         "3s",
		"CONSUMER_BACKOFF_FACTOR":          "1.5",
		"CONSUMER_BACKOFF_MAX":             "90s",
		"SQS_MAX_RECEIVE_COUNT":            "4",
		"PUBLISHER_NAME":                   "some-publisher",
		"PUBLISHER_BATCH":                  "6",
		"PUBLISHER_HOLD":                   "45s",
		"PUBLISHER_INTERVAL":               "2s",
		"PUBLISHER_DRAIN_TIMEOUT":          "12s",
		"PUBLISHER_BACKOFF_INITIAL":        "4s",
		"PUBLISHER_BACKOFF_FACTOR":         "3",
		"PUBLISHER_BACKOFF_MAX":            "4m",
		"REFERENCE_WORKER_NAME":            "some-reference-worker",
		"REFERENCE_WORKER_INTERVAL":        "5s",
		"REFERENCE_WORKER_DRAIN_TIMEOUT":   "13s",
		"REFERENCE_WORKER_BACKOFF_INITIAL": "7s",
		"REFERENCE_WORKER_BACKOFF_FACTOR":  "4",
		"REFERENCE_WORKER_BACKOFF_MAX":     "70s",
	})
	cfg, err := config.Load(config.Static(env))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	consumer := consumerSettings(cfg.Consumer, cfg.SQS)
	publisher := publisherSettings(cfg.Publisher)
	reference := referenceSettings(cfg.Reference)

	for _, check := range []struct {
		what string
		got  any
		want any
	}{
		{"the consumer's name", consumer.Name, "some-consumer"},
		{"the consumer's concurrency", consumer.Concurrency, 7},
		{"the consumer's drain timeout", consumer.DrainTimeout, 11 * time.Second},
		{"the consumer's redrive policy", consumer.MaxReceiveCount, 4},
		{"the consumer's backoff", consumer.Backoff, workers.Backoff{
			Initial: 3 * time.Second, Factor: 1.5, Max: 90 * time.Second,
		}},

		{"the publisher's name", publisher.Name, "some-publisher"},
		{"the publisher's batch", publisher.Batch, 6},
		{"the publisher's hold", publisher.Hold, 45 * time.Second},
		{"the publisher's interval", publisher.Interval, 2 * time.Second},
		{"the publisher's drain timeout", publisher.DrainTimeout, 12 * time.Second},
		{"the publisher's backoff", publisher.Backoff, workers.Backoff{
			Initial: 4 * time.Second, Factor: 3, Max: 4 * time.Minute,
		}},

		{"the reference worker's name", reference.Name, "some-reference-worker"},
		{"the reference worker's interval", reference.Interval, 5 * time.Second},
		{"the reference worker's drain timeout", reference.DrainTimeout, 13 * time.Second},
		{"the reference worker's backoff", reference.Backoff, workers.Backoff{
			Initial: 7 * time.Second, Factor: 4, Max: 70 * time.Second,
		}},
	} {
		if check.got != check.want {
			t.Errorf("%s is %v, want %v", check.what, check.got, check.want)
		}
	}
}
