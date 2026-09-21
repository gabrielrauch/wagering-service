package workers

import (
	"math"
	"strings"
	"testing"
	"time"
)

// A worker built without a dependency would fail at the first message rather
// than at start-up — and by then it has already taken one off the queue.
func TestAConsumerRefusesWhatItCannotWorkWithout(t *testing.T) {
	complete := func() ConsumerConfig {
		return ConsumerConfig{
			Queue: newFakeQueue(), Wagering: &fakeSubmitter{},
			Name: "wager-transactions", Logger: discard(),
		}
	}
	cases := []struct {
		name  string
		spoil func(*ConsumerConfig)
		why   string
	}{
		{name: "no queue", spoil: func(c *ConsumerConfig) { c.Queue = nil }, why: "inbound queue"},
		{
			name:  "no wagering service",
			spoil: func(c *ConsumerConfig) { c.Wagering = nil },
			why:   "wagering service",
		},
		{name: "no logger", spoil: func(c *ConsumerConfig) { c.Logger = nil }, why: "logger"},
		{name: "no name", spoil: func(c *ConsumerConfig) { c.Name = "" }, why: "needs a name"},
		{
			name:  "a name the inbox could not store",
			spoil: func(c *ConsumerConfig) { c.Name = strings.Repeat("c", 129) },
			why:   "not storable",
		},
		{
			name:  "a name carrying a newline",
			spoil: func(c *ConsumerConfig) { c.Name = "wager\nconsumer" },
			why:   "not storable",
		},
		{
			name:  "a negative concurrency",
			spoil: func(c *ConsumerConfig) { c.Concurrency = -1 },
			why:   "positive concurrency",
		},
		{
			name:  "a negative drain timeout",
			spoil: func(c *ConsumerConfig) { c.DrainTimeout = -time.Second },
			why:   "positive drain timeout",
		},
		{
			name:  "a redrive policy that receives less than once",
			spoil: func(c *ConsumerConfig) { c.MaxReceiveCount = -1 },
			why:   "at least once",
		},
		{
			name: "a backoff that backs nothing off",
			spoil: func(c *ConsumerConfig) {
				c.Backoff = Backoff{Initial: time.Minute, Factor: 2, Max: time.Second}
			},
			why: "below its initial delay",
		},
		{
			name: "a backoff longer than a message may be hidden",
			spoil: func(c *ConsumerConfig) {
				c.Backoff = Backoff{Initial: time.Hour, Factor: 2, Max: 13 * time.Hour}
			},
			why: "hidden for at most",
		},
		{
			name: "a backoff factor that is not a number",
			spoil: func(c *ConsumerConfig) {
				c.Backoff = Backoff{Initial: time.Second, Factor: math.NaN(), Max: time.Minute}
			},
			why: "is not a number",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := complete()
			c.spoil(&cfg)
			consumer, err := NewConsumer(cfg)
			if err == nil {
				t.Fatal("a consumer was built that should have been refused")
			}
			if consumer != nil {
				t.Error("a refused consumer was returned anyway")
			}
			if !strings.Contains(err.Error(), c.why) {
				t.Errorf("refusal = %q, want it to say %q", err, c.why)
			}
		})
	}
}

func TestAConsumerFillsInWhatItWasNotTold(t *testing.T) {
	consumer, err := NewConsumer(ConsumerConfig{
		Queue: newFakeQueue(), Wagering: &fakeSubmitter{},
		Name: "wager-transactions", Logger: discard(),
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if consumer.concurrency != defaultConcurrency {
		t.Errorf("concurrency = %d, want %d", consumer.concurrency, defaultConcurrency)
	}
	if consumer.drain != defaultDrainTimeout {
		t.Errorf("drain = %s, want %s", consumer.drain, defaultDrainTimeout)
	}
	want := Backoff{
		Initial: defaultConsumerInitial,
		Factor:  defaultConsumerFactor,
		Max:     defaultConsumerMax,
	}
	if consumer.backoff != want {
		t.Errorf("backoff = %+v, want %+v", consumer.backoff, want)
	}
}

func TestAPublisherRefusesWhatItCannotWorkWithout(t *testing.T) {
	complete := func() PublisherConfig {
		return PublisherConfig{
			Outbox: newFakeOutbox(), Queue: &fakeSender{},
			Clock: fixedClock{at: testTime()}, Name: "publisher-1", Logger: discard(),
		}
	}
	cases := []struct {
		name  string
		spoil func(*PublisherConfig)
		why   string
	}{
		{name: "no outbox", spoil: func(c *PublisherConfig) { c.Outbox = nil }, why: "the outbox"},
		{name: "no queue", spoil: func(c *PublisherConfig) { c.Queue = nil }, why: "outbound queue"},
		{name: "no clock", spoil: func(c *PublisherConfig) { c.Clock = nil }, why: "clock"},
		{name: "no logger", spoil: func(c *PublisherConfig) { c.Logger = nil }, why: "logger"},
		{
			name:  "no name to claim under",
			spoil: func(c *PublisherConfig) { c.Name = "" },
			why:   "name to claim under",
		},
		{
			name:  "a negative batch",
			spoil: func(c *PublisherConfig) { c.Batch = -1 },
			why:   "positive batch",
		},
		{
			name:  "a negative hold",
			spoil: func(c *PublisherConfig) { c.Hold = -time.Second },
			why:   "positive claim hold",
		},
		{
			name:  "a negative interval",
			spoil: func(c *PublisherConfig) { c.Interval = -time.Second },
			why:   "positive poll interval",
		},
		{
			name:  "a negative drain timeout",
			spoil: func(c *PublisherConfig) { c.DrainTimeout = -time.Second },
			why:   "positive drain timeout",
		},
		{
			name:  "a batch larger than one send can carry",
			spoil: func(c *PublisherConfig) { c.Batch = maxClaimBatch + 1 },
			why:   "past the 10 one send can carry",
		},
		{
			name: "a backoff factor that shortens each wait",
			spoil: func(c *PublisherConfig) {
				c.Backoff = Backoff{Initial: time.Second, Factor: 0.5, Max: time.Minute}
			},
			why: "publisher backoff factor below 1",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := complete()
			c.spoil(&cfg)
			publisher, err := NewPublisher(cfg)
			if err == nil {
				t.Fatal("a publisher was built that should have been refused")
			}
			if publisher != nil {
				t.Error("a refused publisher was returned anyway")
			}
			if !strings.Contains(err.Error(), c.why) {
				t.Errorf("refusal = %q, want it to say %q", err, c.why)
			}
		})
	}
}

func TestAPublisherFillsInWhatItWasNotTold(t *testing.T) {
	publisher, err := NewPublisher(PublisherConfig{
		Outbox: newFakeOutbox(), Queue: &fakeSender{},
		Clock: fixedClock{at: testTime()}, Name: "publisher-1", Logger: discard(),
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if publisher.batch != defaultClaimBatch {
		t.Errorf("batch = %d, want %d", publisher.batch, defaultClaimBatch)
	}
	if publisher.hold != defaultClaimHold {
		t.Errorf("hold = %s, want %s", publisher.hold, defaultClaimHold)
	}
	if publisher.interval != defaultPollInterval {
		t.Errorf("interval = %s, want %s", publisher.interval, defaultPollInterval)
	}
	if publisher.drain != defaultDrainTimeout {
		t.Errorf("drain = %s, want %s", publisher.drain, defaultDrainTimeout)
	}
	want := Backoff{
		Initial: defaultPublisherInitial,
		Factor:  defaultPublisherFactor,
		Max:     defaultPublisherMax,
	}
	if publisher.backoff != want {
		t.Errorf("backoff = %+v, want %+v", publisher.backoff, want)
	}
}

func TestAReferenceWorkerRefusesWhatItCannotWorkWithout(t *testing.T) {
	complete := func() ReferenceConfig {
		return ReferenceConfig{
			Wagering: newFakeResumer(nil), Name: "reference-worker", Logger: discard(),
		}
	}
	cases := []struct {
		name  string
		spoil func(*ReferenceConfig)
		why   string
	}{
		{
			name:  "no wagering service",
			spoil: func(c *ReferenceConfig) { c.Wagering = nil },
			why:   "wagering service",
		},
		{name: "no logger", spoil: func(c *ReferenceConfig) { c.Logger = nil }, why: "logger"},
		{
			name:  "no subject to act under",
			spoil: func(c *ReferenceConfig) { c.Name = "" },
			why:   "needs a principal",
		},
		{
			name:  "a negative interval",
			spoil: func(c *ReferenceConfig) { c.Interval = -time.Second },
			why:   "positive interval",
		},
		{
			name:  "a negative drain timeout",
			spoil: func(c *ReferenceConfig) { c.DrainTimeout = -time.Second },
			why:   "positive drain timeout",
		},
		{
			name: "a backoff with no initial delay",
			spoil: func(c *ReferenceConfig) {
				c.Backoff = Backoff{Factor: 2, Max: time.Minute}
			},
			why: "reference worker backoff needs a positive initial delay",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := complete()
			c.spoil(&cfg)
			worker, err := NewReferenceWorker(cfg)
			if err == nil {
				t.Fatal("a reference worker was built that should have been refused")
			}
			if worker != nil {
				t.Error("a refused worker was returned anyway")
			}
			if !strings.Contains(err.Error(), c.why) {
				t.Errorf("refusal = %q, want it to say %q", err, c.why)
			}
		})
	}
}

func TestAReferenceWorkerFillsInWhatItWasNotTold(t *testing.T) {
	worker, err := NewReferenceWorker(ReferenceConfig{
		Wagering: newFakeResumer(nil), Name: "reference-worker", Logger: discard(),
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if worker.interval != defaultResumeInterval {
		t.Errorf("interval = %s, want %s", worker.interval, defaultResumeInterval)
	}
	if worker.drain != defaultDrainTimeout {
		t.Errorf("drain = %s, want %s", worker.drain, defaultDrainTimeout)
	}
	want := Backoff{
		Initial: defaultResumeInitial,
		Factor:  defaultResumeFactor,
		Max:     defaultResumeMax,
	}
	if worker.backoff != want {
		t.Errorf("backoff = %+v, want %+v", worker.backoff, want)
	}
}

// Nothing a constructor is given makes it talk to anything. A queue that does
// not exist is a start-up failure with an operator watching, not a constructor
// that hung.
func TestBuildingAWorkerTouchesNothing(t *testing.T) {
	queue := newFakeQueue()
	outbox := newFakeOutbox()
	resumer := newFakeResumer(nil)

	if _, err := NewConsumer(ConsumerConfig{
		Queue: queue, Wagering: &fakeSubmitter{}, Name: "wager-transactions", Logger: discard(),
	}); err != nil {
		t.Fatalf("build a consumer: %v", err)
	}
	if _, err := NewPublisher(PublisherConfig{
		Outbox: outbox, Queue: &fakeSender{}, Clock: fixedClock{at: testTime()},
		Name: "publisher-1", Logger: discard(),
	}); err != nil {
		t.Fatalf("build a publisher: %v", err)
	}
	if _, err := NewReferenceWorker(ReferenceConfig{
		Wagering: resumer, Name: "reference-worker", Logger: discard(),
	}); err != nil {
		t.Fatalf("build a reference worker: %v", err)
	}

	if got := queue.receiveCount(); got != 0 {
		t.Errorf("the consumer's constructor received %d times", got)
	}
	if got := len(outbox.claimed()); got != 0 {
		t.Errorf("the publisher's constructor claimed %d times", got)
	}
	if got := resumer.count(); got != 0 {
		t.Errorf("the reference worker's constructor resumed %d times", got)
	}
}
