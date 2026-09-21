// Package faults kills this process at a named point, so that a recovery test
// can prove what survives a crash instead of arguing about it.
//
// It is inert unless FAULT_POINT names one of the points below, which is the
// whole of its production behaviour: every call site is one map lookup on an
// environment that is already in memory, sitting between two pieces of I/O.
// Nothing here is wired to a flag, a configuration file or a build tag, because
// a fault injector that could be switched on by a deployment's own
// configuration is a fault injector that will be.
//
// # What it is for
//
// The four points are the four windows in which this system can lose or
// duplicate work, and each one exists because something outside the database
// has to happen after a transaction commits:
//
//   - [AfterCommitBeforeAck] — the consumer has committed the wager transaction
//     and has not yet deleted the message. A process that dies here must see
//     the message again and must not apply it twice; the inbox is what makes
//     that true.
//   - [AfterClaimBeforePublish] — the publisher holds an outbox claim and has
//     sent nothing. A process that dies here must not hold the event hostage;
//     the claim expiring by wall clock is what makes that true.
//   - [AfterPublishBeforeMark] — the publisher has put the event on the queue
//     and has not recorded that it did. A process that dies here republishes;
//     the deduplication id being the event id is what keeps that from becoming
//     a second event on the wire.
//   - [AfterPendingCommit] — the reference worker has committed an operation's
//     parked state and has not done whatever follows. A process that dies here
//     must find the operation again when it comes back.
//
// # The decisions
//
// The environment is read on every call rather than once at start-up. Reading
// it once would need an init function or a sync.Once, and it would make a test
// that arms a fault after the process is running behave differently from one
// that arms it before — which is exactly the difference a recovery test must
// not have to think about.
//
// One line goes to standard error before the exit. A process that vanishes with
// no explanation is indistinguishable from a crash at the worst possible
// moment, which is precisely the moment this package chooses; the line is what
// tells whoever is reading the logs that the death was asked for.
//
// FAULT_POINT set to something no point uses is reported once, also on standard
// error, and then ignored. The constants make a mistyped call site a compile
// error; nothing makes a mistyped environment variable one, and without the
// warning a recovery test armed at "after_commit_before_akc" would pass by
// never crashing at all.
//
// The exit status is [ExitCode]. See its own comment for why that number.
package faults

import (
	"fmt"
	"os"
	"sync"
)

// Variable is the environment variable that arms a fault point. It names at
// most one: a run in which two points could fire would settle a recovery test's
// outcome by whichever the scheduler reached first.
const Variable = "FAULT_POINT"

// ExitCode is the status a process exits with when a fault point fires.
//
// 99 rather than 1, and rather than any number anything else in this tree
// produces. 0 is success, 1 is what `go test` and most failed commands report,
// 2 is what the Go runtime reports for a panic, and 128+n is a signal — 130 for
// an interrupt, 143 for a termination. 99 is below 125, where a shell's own
// meanings begin, and is not any of those, so a test that asserts on it is
// asserting that the fault fired and not that the process died some other way.
const ExitCode = 99

// The points this system can be killed at. Each is the instant after a
// transaction has committed and before the effect outside the database that
// was supposed to follow it.
const (
	// AfterCommitBeforeAck is the consumer, between the commit and the delete.
	AfterCommitBeforeAck = "after_commit_before_ack"
	// AfterPublishBeforeMark is the publisher, between the send and the outbox
	// row being marked published.
	AfterPublishBeforeMark = "after_publish_before_mark"
	// AfterClaimBeforePublish is the publisher, between taking the claim and
	// sending anything.
	AfterClaimBeforePublish = "after_claim_before_publish"
	// AfterPendingCommit is the reference worker, after the commit that parked
	// or resumed an operation.
	AfterPendingCommit = "after_pending_commit"
)

// points is every name [Hit] will act on, so that a FAULT_POINT nobody
// implements can be reported rather than silently doing nothing.
var points = map[string]bool{
	AfterCommitBeforeAck:    true,
	AfterPublishBeforeMark:  true,
	AfterClaimBeforePublish: true,
	AfterPendingCommit:      true,
}

// warnOnce keeps the report of an unrecognised FAULT_POINT to one line, because
// these calls sit in loops and a warning per message would bury the logs the
// recovery test is reading.
var warnOnce sync.Once

// Hit ends this process when FAULT_POINT names it, and does nothing otherwise.
//
// The comparison is exact. A name is one of the constants above; passing a
// string literal compiles, which is why an unrecognised FAULT_POINT is reported
// rather than ignored.
func Hit(name string) {
	armed := os.Getenv(Variable)
	if armed == "" {
		return
	}
	if armed == name {
		fmt.Fprintf(os.Stderr, "faults: %s=%s fired, exiting with %d\n", Variable, name, ExitCode)
		os.Exit(ExitCode)
	}
	if !points[armed] {
		warnOnce.Do(func() {
			fmt.Fprintf(os.Stderr,
				"faults: %s=%q names no fault point, so none will ever fire\n", Variable, armed)
		})
	}
}
