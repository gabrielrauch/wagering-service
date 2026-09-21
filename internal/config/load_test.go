package config

import (
	"bufio"
	"log/slog"
	"maps"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// minimal is the smallest environment this service starts from: the four
// required variables and the publisher's name, which has no default.
//
// Every test below starts from a copy of it, so a variable that becomes
// required is a failure in every case rather than in whichever one happened to
// set it.
func minimal() map[string]string {
	return map[string]string{
		"DATABASE_URL":   "postgres://app:secret@db:5432/wagering?sslmode=disable",
		"AWS_REGION":     "us-east-1",
		"OIDC_ISSUER":    "http://localhost:8080/realms/wagering",
		"OIDC_AUDIENCE":  "wagering-api",
		"PUBLISHER_NAME": "publisher-1",
	}
}

// with returns the minimal environment, overridden.
func with(overrides map[string]string) map[string]string {
	env := minimal()
	maps.Copy(env, overrides)
	return env
}

// loadFrom is Load against a map, which is how every case here reads its
// environment: nothing in this package touches the process's own.
func loadFrom(t *testing.T, env map[string]string) (Config, error) {
	t.Helper()
	return Load(Static(env))
}

func TestLoadAcceptsTheMinimalEnvironment(t *testing.T) {
	t.Parallel()

	cfg, err := loadFrom(t, minimal())
	if err != nil {
		t.Fatalf("load the minimal environment: %v", err)
	}

	// Spot-checked across every section, because a default that is silently
	// zero is the failure this asserts against: a zero duration is "no bound"
	// to one constructor and "your default" to another.
	for _, check := range []struct {
		what string
		got  any
		want any
	}{
		{"service name", cfg.Telemetry.ServiceName, defaultServiceName},
		{"log level", cfg.Telemetry.LogLevel, slog.LevelInfo},
		{"log format", cfg.Telemetry.LogFormat, LogJSON},
		{"telemetry shutdown", cfg.Telemetry.ShutdownTimeout, defaultTelemetryShutdown},
		{"start timeout", cfg.Lifecycle.StartTimeout, defaultStartTimeout},
		{"stop timeout", cfg.Lifecycle.StopTimeout, defaultStopTimeout},
		{"pool limit", cfg.Postgres.MaxConns, int32(defaultMaxConns)},
		{"lock timeout", cfg.Postgres.LockTimeout, defaultLockTimeout},
		{"inbound queue", cfg.SQS.InboundQueue, defaultInboundQueue},
		{"outbound queue", cfg.SQS.OutboundQueue, defaultOutboundQueue},
		{"receive batch", cfg.SQS.MaxMessages, defaultMaxMessages},
		{"redrive policy", cfg.SQS.MaxReceiveCount, defaultMaxReceiveCount},
		{"visibility timeout", cfg.SQS.VisibilityTimeout, defaultVisibilityTimeout},
		{"clock skew", cfg.OIDC.ClockSkew, defaultClockSkew},
		{"listen address", cfg.HTTP.Addr, defaultAddr},
		{"body limit", cfg.HTTP.MaxBodyBytes, int64(defaultMaxBodyBytes)},
		{"reference attempts", cfg.Wagering.ReferenceAttempts, defaultReferenceAttempts},
		{"reference budget", cfg.Wagering.ReferenceTTL, defaultReferenceTTL},
		{"wagering backoff", cfg.Wagering.Backoff, defaultWageringBackoff},
		{"consumer name", cfg.Consumer.Name, defaultConsumerName},
		{"consumer concurrency", cfg.Consumer.Concurrency, defaultConcurrency},
		{"consumer backoff", cfg.Consumer.Backoff, defaultConsumerBackoff},
		{"publisher batch", cfg.Publisher.Batch, defaultClaimBatch},
		{"publisher backoff", cfg.Publisher.Backoff, defaultPublisherBackoff},
		{"reference worker name", cfg.Reference.Name, defaultReferenceName},
		{"reference worker backoff", cfg.Reference.Backoff, defaultReferenceBackoff},
	} {
		if check.got != check.want {
			t.Errorf("%s is %v, want %v", check.what, check.got, check.want)
		}
	}

	for _, enabled := range []struct {
		what string
		got  bool
	}{
		{"consumer", cfg.Consumer.Enabled},
		{"publisher", cfg.Publisher.Enabled},
		{"reference worker", cfg.Reference.Enabled},
	} {
		if !enabled.got {
			t.Errorf("the %s is disabled by default, want enabled", enabled.what)
		}
	}
}

func TestLoadRefusesEveryMissingRequiredVariable(t *testing.T) {
	t.Parallel()

	// Named individually rather than derived, so that a variable losing its
	// requirement is a change somebody makes here on purpose.
	required := []string{
		"DATABASE_URL",
		"AWS_REGION",
		"OIDC_ISSUER",
		"OIDC_AUDIENCE",
		"PUBLISHER_NAME",
	}
	for _, key := range required {
		t.Run(key, func(t *testing.T) {
			t.Parallel()

			env := minimal()
			delete(env, key)
			_, err := loadFrom(t, env)
			if err == nil {
				t.Fatalf("%s was absent and the configuration loaded anyway", key)
			}
			if !strings.Contains(err.Error(), key) {
				t.Fatalf("%s was absent and the refusal did not name it: %v", key, err)
			}
		})
	}
}

func TestLoadTreatsAnEmptyVariableAsUnset(t *testing.T) {
	t.Parallel()

	t.Run("a required one is missing", func(t *testing.T) {
		t.Parallel()

		_, err := loadFrom(t, with(map[string]string{"AWS_REGION": ""}))
		if err == nil || !strings.Contains(err.Error(), "AWS_REGION") {
			t.Fatalf("an empty AWS_REGION was accepted: %v", err)
		}
	})

	t.Run("an optional one keeps its default", func(t *testing.T) {
		t.Parallel()

		// The case this rule exists for: `FOO=${BAR}` in a compose file with no
		// BAR. Reading it as "set to empty" would make the pool one connection
		// wide and the queue name the empty string.
		cfg, err := loadFrom(t, with(map[string]string{
			"DATABASE_MAX_CONNS": "",
			"SQS_INBOUND_QUEUE":  "   ",
			"CONSUMER_ENABLED":   "",
		}))
		if err != nil {
			t.Fatalf("load with empty optional variables: %v", err)
		}
		if cfg.Postgres.MaxConns != defaultMaxConns {
			t.Errorf("pool limit is %d, want the default %d", cfg.Postgres.MaxConns,
				defaultMaxConns)
		}
		if cfg.SQS.InboundQueue != defaultInboundQueue {
			t.Errorf("inbound queue is %q, want the default %q", cfg.SQS.InboundQueue,
				defaultInboundQueue)
		}
		if !cfg.Consumer.Enabled {
			t.Error("an empty CONSUMER_ENABLED switched the consumer off")
		}
	})
}

func TestLoadRefusesMalformedValues(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		key   string
		value string
		says  string
	}{
		{"a duration that is not one", "DATABASE_LOCK_TIMEOUT", "3", "is not a duration"},
		{"a duration of zero", "DATABASE_LOCK_TIMEOUT", "0s", "must be positive"},
		{"a negative duration", "HTTP_READ_TIMEOUT", "-1s", "must be positive"},
		{"a count that is not a number", "CONSUMER_CONCURRENCY", "many",
			"is not a whole number"},
		{"a count of zero", "CONSUMER_CONCURRENCY", "0", "must be between"},
		{"a negative count", "SQS_MAX_MESSAGES", "-1", "must be between"},
		{"a count past what it is narrowed to", "DATABASE_MAX_CONNS", "2147483648",
			"must be between"},
		{"a size that is not a number", "HTTP_MAX_BODY_BYTES", "1MiB",
			"is not a whole number"},
		{"a factor that is not a number", "CONSUMER_BACKOFF_FACTOR", "twice",
			"is not a number"},
		{"a switch that is not one", "PUBLISHER_ENABLED", "yes", "is not true or false"},
		{"a level that is not one", "LOG_LEVEL", "chatty", "is not a level"},
		{"a format that is not one", "LOG_FORMAT", "xml", "is not a log format"},
		{"an allow-list with an empty entry", "OIDC_ALGORITHMS", "RS256,,ES256",
			"has an empty entry"},
		{"an allow-list that is only a comma", "OIDC_ALGORITHMS", ",", "has an empty entry"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := loadFrom(t, with(map[string]string{tc.key: tc.value}))
			if err == nil {
				t.Fatalf("%s=%q was accepted", tc.key, tc.value)
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Errorf("the refusal did not name %s: %v", tc.key, err)
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Errorf("the refusal did not say %q: %v", tc.says, err)
			}
		})
	}
}

// TestLoadRefusesANotANumberFactor is the case this package exists to catch.
//
// strconv.ParseFloat accepts every one of these spellings, and app.BackoffPolicy
// would too: its only check is Factor < 1, and NaN is not less than anything. A
// NaN reaching it makes math.Pow return NaN, min return NaN, and the conversion
// to a Duration return a wait in the past — so the reference worker looks at the
// same parked operation as fast as it can, for ever.
func TestLoadRefusesANotANumberFactor(t *testing.T) {
	t.Parallel()

	// Every backoff read here, because closing it for one and not the others is
	// the way this comes back.
	prefixes := []string{"WAGERING", "CONSUMER", "PUBLISHER", "REFERENCE_WORKER"}
	spellings := []string{"NaN", "nan", "NAN", "+Inf", "-Inf", "inf"}

	for _, prefix := range prefixes {
		key := prefix + "_BACKOFF_FACTOR"
		for _, spelling := range spellings {
			t.Run(key+"="+spelling, func(t *testing.T) {
				t.Parallel()

				// The premise, asserted rather than assumed: these are exactly
				// the values that get past strconv and past a "< 1" check.
				if got, err := parseFloatForTest(spelling); err != nil {
					t.Fatalf("strconv refused %q, so this case is not the hazard "+
						"it was written for: %v", spelling, err)
				} else if got < 1 && !math.IsInf(got, -1) {
					t.Fatalf("%q parses to %v, which a factor check would catch on its "+
						"own", spelling, got)
				}

				_, err := loadFrom(t, with(map[string]string{key: spelling}))
				if err == nil {
					t.Fatalf("%s=%s was accepted", key, spelling)
				}
				if !strings.Contains(err.Error(), key) {
					t.Errorf("the refusal did not name %s: %v", key, err)
				}
				if !strings.Contains(err.Error(), "finite") {
					t.Errorf("the refusal did not say the value is not finite: %v", err)
				}
			})
		}
	}
}

// parseFloatForTest is strconv.ParseFloat, named so the assertion above reads
// as the premise it is.
func parseFloatForTest(s string) (float64, error) { return strconv.ParseFloat(s, 64) }

func TestLoadReportsEveryProblemAtOnce(t *testing.T) {
	t.Parallel()

	env := with(map[string]string{
		"CONSUMER_CONCURRENCY":  "0",
		"LOG_LEVEL":             "chatty",
		"PUBLISHER_BACKOFF_MAX": "not-a-duration",
	})
	delete(env, "AWS_REGION")

	_, err := loadFrom(t, env)
	if err == nil {
		t.Fatal("four wrong variables were accepted")
	}
	for _, key := range []string{
		"AWS_REGION", "CONSUMER_CONCURRENCY", "LOG_LEVEL", "PUBLISHER_BACKOFF_MAX",
	} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("the refusal did not name %s, so an operator would find it on the "+
				"next restart rather than this one: %v", key, err)
		}
	}
}

func TestPublisherNameIsDistinctPerProcess(t *testing.T) {
	t.Parallel()

	t.Run("PUBLISHER_NAME wins", func(t *testing.T) {
		t.Parallel()

		cfg, err := loadFrom(t, with(map[string]string{
			"PUBLISHER_NAME": "chosen", "HOSTNAME": "pod-7",
		}))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if cfg.Publisher.Name != "chosen" {
			t.Errorf("publisher name is %q, want %q", cfg.Publisher.Name, "chosen")
		}
	})

	t.Run("HOSTNAME stands in", func(t *testing.T) {
		t.Parallel()

		env := minimal()
		delete(env, "PUBLISHER_NAME")
		env["HOSTNAME"] = "pod-7"

		cfg, err := loadFrom(t, env)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if cfg.Publisher.Name != "pod-7" {
			t.Errorf("publisher name is %q, want the hostname %q", cfg.Publisher.Name, "pod-7")
		}
	})

	t.Run("neither is refused", func(t *testing.T) {
		t.Parallel()

		env := minimal()
		delete(env, "PUBLISHER_NAME")

		_, err := loadFrom(t, env)
		if err == nil {
			t.Fatal("a publisher with no distinct name was accepted, which lets two " +
				"replicas reschedule each other's outbox claims")
		}
		if !strings.Contains(err.Error(), "HOSTNAME") {
			t.Errorf("the refusal did not mention the fallback: %v", err)
		}
	})
}

// TestConsumerNameIsNotDerived is the other half of the asymmetry.
//
// A consumer name that varied per process would give each replica its own inbox
// rows, so a message redelivered to a different replica would be applied twice.
// The assertion is that nothing ambient reaches it: the same configuration with
// a different HOSTNAME is the same consumer.
func TestConsumerNameIsNotDerived(t *testing.T) {
	t.Parallel()

	// Two replicas of one deployment: the same environment but for the one
	// thing a container runtime makes distinct.
	replica := func(host string) Config {
		t.Helper()
		env := minimal()
		delete(env, "PUBLISHER_NAME")
		env["HOSTNAME"] = host
		cfg, err := loadFrom(t, env)
		if err != nil {
			t.Fatalf("load for %s: %v", host, err)
		}
		return cfg
	}
	first, second := replica("pod-1"), replica("pod-2")

	if first.Consumer.Name != second.Consumer.Name {
		t.Errorf("two replicas are consumers %q and %q; a redelivery to the other one "+
			"would be applied a second time", first.Consumer.Name, second.Consumer.Name)
	}
	if first.Reference.Name != second.Reference.Name {
		t.Errorf("two replicas name the reference worker %q and %q in the audit trail",
			first.Reference.Name, second.Reference.Name)
	}
	if first.Publisher.Name == second.Publisher.Name {
		t.Errorf("two replicas are both publisher %q; each would reschedule the other's "+
			"claims", first.Publisher.Name)
	}
}

func TestLoadNeedsASource(t *testing.T) {
	t.Parallel()

	if _, err := Load(nil); err == nil {
		t.Fatal("Load(nil) built a configuration out of nothing")
	}
}

func TestRedactedKeepsThePasswordOut(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "a password is replaced",
			dsn:  "postgres://app:hunter2@db:5432/wagering?sslmode=disable",
			want: "postgres://app:xxxxx@db:5432/wagering?sslmode=disable",
		},
		{
			name: "a DSN without one is unchanged",
			dsn:  "postgres://db:5432/wagering",
			want: "postgres://db:5432/wagering",
		},
		{
			name: "one that will not parse says nothing",
			dsn:  "postgres://app:hunter2@db:5432/%zz",
			want: "<unparseable dsn>",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := Postgres{DSN: tc.dsn}.Redacted()
			if got != tc.want {
				t.Errorf("redacted to %q, want %q", got, tc.want)
			}
			if strings.Contains(got, "hunter2") {
				t.Errorf("the password survived redaction: %q", got)
			}
		})
	}
}

// TestARefusalNeverQuotesTheDSN covers the other way a password escapes: not
// through a deliberate render, but through an error message that happens to
// quote the value it was given.
//
// The DSN is present and valid here and something else is wrong, because that
// is the shape that matters — a load that fails for any reason returns every
// message at once, and a start-up that prints that error prints whatever is in
// it.
func TestARefusalNeverQuotesTheDSN(t *testing.T) {
	t.Parallel()

	const password = "hunter2"
	_, err := loadFrom(t, with(map[string]string{
		"DATABASE_URL":       "postgres://app:" + password + "@db:5432/wagering",
		"DATABASE_MAX_CONNS": "0",
		"LOG_LEVEL":          "chatty",
	}))
	if err == nil {
		t.Fatal("two wrong variables were accepted")
	}
	if strings.Contains(err.Error(), password) {
		t.Errorf("the refusal carried the password: %v", err)
	}
}

// TestExampleEnvironmentMatchesTheLoader keeps .env.example honest.
//
// It is the file an operator copies, so a variable this package reads and the
// file does not mention is a default nobody can find, and a variable the file
// mentions and this package does not read is a line somebody will set and
// wonder about. Both directions are checked by recording what Load asks for.
func TestExampleEnvironmentMatchesTheLoader(t *testing.T) {
	t.Parallel()

	// Read by the AWS SDK's own credential chain rather than by this package,
	// and in the file because a local stack does not work without them.
	sdkOwned := map[string]bool{
		"AWS_ACCESS_KEY_ID":         true,
		"AWS_SECRET_ACCESS_KEY":     true,
		"AWS_EC2_METADATA_DISABLED": true,
	}

	example := readExample(t, "../../.env.example")

	asked := map[string]bool{}
	recording := func(key string) (string, bool) {
		asked[key] = true
		value, present := example[key]
		return value, present
	}
	if _, err := Load(recording); err != nil {
		t.Fatalf(".env.example does not load: %v", err)
	}

	for key := range asked {
		if _, ok := example[key]; !ok {
			t.Errorf("%s is read by this package and is not in .env.example", key)
		}
	}
	for key := range example {
		if !asked[key] && !sdkOwned[key] {
			t.Errorf("%s is in .env.example and is read by nothing", key)
		}
	}
}

// readExample parses the example file the way a shell would read a plain
// KEY=value file: no expansion, no quoting, comments and blank lines skipped.
func readExample(t *testing.T, path string) map[string]string {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open the example environment: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })

	values := map[string]string{}
	scanner := bufio.NewScanner(file)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		key, value, ok := strings.Cut(text, "=")
		if !ok {
			t.Fatalf("%s:%d is neither a comment nor a KEY=value line: %q", path, line, text)
		}
		values[strings.TrimSpace(key)] = value
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read the example environment: %v", err)
	}
	return values
}

// durationsAreParsedInGoNotation pins the notation the example file is written
// in, so that a value an operator copies out of it means what it looks like.
func TestDurationsAreParsedInGoNotation(t *testing.T) {
	t.Parallel()

	cfg, err := loadFrom(t, with(map[string]string{
		"HTTP_READ_TIMEOUT":      "250ms",
		"REFERENCE_TTL":          "1h30m",
		"SQS_WAIT_TIME":          "20s",
		"CONSUMER_DRAIN_TIMEOUT": "20s",
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	for _, check := range []struct {
		what string
		got  time.Duration
		want time.Duration
	}{
		{"read timeout", cfg.HTTP.ReadTimeout, 250 * time.Millisecond},
		{"reference budget", cfg.Wagering.ReferenceTTL, 90 * time.Minute},
		{"wait time", cfg.SQS.WaitTime, 20 * time.Second},
		{"drain timeout", cfg.Consumer.DrainTimeout, 20 * time.Second},
	} {
		if check.got != check.want {
			t.Errorf("%s is %s, want %s", check.what, check.got, check.want)
		}
	}
}
