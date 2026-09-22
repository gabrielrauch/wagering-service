package faults

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// The environment the child process is driven by.
//
// A fault point ends the process it fires in, so the firing path cannot be
// tested in the process running the assertions. The test binary re-executes
// itself with one test selected and these two variables set: hitVariable says
// which point the child calls, and [Variable] arms whichever one the case is
// about. The parent then reads the exit status, which is the whole of what a
// recovery test in step 6 will read.
const (
	hitVariable = "FAULTS_TEST_HIT"
	selfTest    = "^TestHit$"
)

func TestHit(t *testing.T) {
	if point := os.Getenv(hitVariable); point != "" {
		Hit(point)
		// Reached only when the fault did not fire, which is the outcome the
		// parent reads as exit status 0.
		return
	}

	cases := []struct {
		name     string
		armed    string
		hit      string
		exitCode int
		stderr   string
	}{
		{
			name:     "nothing armed does nothing",
			armed:    "",
			hit:      AfterCommitBeforeAck,
			exitCode: 0,
		},
		{
			name:     "the armed point fires",
			armed:    AfterCommitBeforeAck,
			hit:      AfterCommitBeforeAck,
			exitCode: ExitCode,
			stderr:   "after_commit_before_ack",
		},
		{
			name:     "another point armed leaves this one alone",
			armed:    AfterPublishBeforeMark,
			hit:      AfterCommitBeforeAck,
			exitCode: 0,
		},
		{
			name:     "a point nobody implements is reported",
			armed:    "after_commit_before_akc",
			hit:      AfterCommitBeforeAck,
			exitCode: 0,
			stderr:   "names no fault point",
		},
		{
			name:     "the publisher's two points fire independently",
			armed:    AfterClaimBeforePublish,
			hit:      AfterClaimBeforePublish,
			exitCode: ExitCode,
			stderr:   "after_claim_before_publish",
		},
		{
			name:     "the reference worker's point fires",
			armed:    AfterPendingCommit,
			hit:      AfterPendingCommit,
			exitCode: ExitCode,
			stderr:   "after_pending_commit",
		},
		{
			// The point step 6's publisher-recovery test arms. It fires in a
			// case of its own rather than only appearing as the "some other
			// point" of the case above, so that all five are exercised the
			// same way.
			name:     "the publisher's mark point fires",
			armed:    AfterPublishBeforeMark,
			hit:      AfterPublishBeforeMark,
			exitCode: ExitCode,
			stderr:   "after_publish_before_mark",
		},
		{
			name:     "the movement's point before the commit fires",
			armed:    BeforeCommit,
			hit:      BeforeCommit,
			exitCode: ExitCode,
			stderr:   "before_commit",
		},
		{
			// The two points either side of a commit are different points: a
			// process armed before the commit must not die after it.
			name:     "the point before the commit leaves the one after it alone",
			armed:    BeforeCommit,
			hit:      AfterCommitBeforeAck,
			exitCode: 0,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, stderr := runChild(t, c.armed, c.hit)
			if code != c.exitCode {
				t.Errorf("exit status = %d, want %d (stderr: %s)", code, c.exitCode, stderr)
			}
			if c.stderr == "" {
				if strings.Contains(stderr, "faults:") {
					t.Errorf("stderr said %q, want nothing from this package", stderr)
				}
				return
			}
			if !strings.Contains(stderr, c.stderr) {
				t.Errorf("stderr = %q, want it to mention %q", stderr, c.stderr)
			}
		})
	}
}

// runChild re-executes the test binary with one fault point armed and reports
// how it ended.
func runChild(t *testing.T, armed, hit string) (int, string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run="+selfTest)
	cmd.Env = append(os.Environ(), hitVariable+"="+hit, Variable+"="+armed)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()

	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, stderr.String()
	case errors.As(err, &exit):
		return exit.ExitCode(), stderr.String()
	default:
		t.Fatalf("run the child: %v", err)
		return 0, ""
	}
}

// TestHitIsANoOpInProcess is the other half of the table above: surviving the
// call IS the assertion, because a fault that fired would take this test binary
// with it.
func TestHitIsANoOpInProcess(t *testing.T) {
	t.Setenv(Variable, "")
	Hit(AfterCommitBeforeAck)

	t.Setenv(Variable, AfterPendingCommit)
	Hit(AfterCommitBeforeAck)
}

// TestThePointsAreSpeltAsTheEnvironmentSpellsThem pins the literal strings.
//
// Nothing else can. A constant is both the declaration and every use of it —
// the map key, the call site, the test table — so renaming the value renames it
// everywhere at once and every test still passes, while the recovery script
// that exports FAULT_POINT=after_publish_before_mark quietly stops working.
// The right-hand sides below are deliberately literals, and this is the one
// place in the package where that is deliberate.
func TestThePointsAreSpeltAsTheEnvironmentSpellsThem(t *testing.T) {
	cases := []struct {
		constant string
		spelling string
	}{
		{AfterCommitBeforeAck, "after_commit_before_ack"},
		{AfterPublishBeforeMark, "after_publish_before_mark"},
		{AfterClaimBeforePublish, "after_claim_before_publish"},
		{AfterPendingCommit, "after_pending_commit"},
		{BeforeCommit, "before_commit"},
	}
	for _, c := range cases {
		if c.constant != c.spelling {
			t.Errorf("a fault point is spelt %q, want %q: an environment naming the old "+
				"spelling would arm nothing", c.constant, c.spelling)
		}
	}
	if Variable != "FAULT_POINT" {
		t.Errorf("the variable is %q, want FAULT_POINT", Variable)
	}
}

// TestEveryPointIsRegistered guards the half of the typo defence that the
// compiler cannot: a constant added above without an entry in points would make
// FAULT_POINT naming it report "names no fault point" while still firing, which
// is the most confusing answer available.
func TestEveryPointIsRegistered(t *testing.T) {
	declared := []string{
		AfterCommitBeforeAck,
		AfterPublishBeforeMark,
		AfterClaimBeforePublish,
		AfterPendingCommit,
		BeforeCommit,
	}
	if len(points) != len(declared) {
		t.Errorf("points holds %d names, but %d are declared", len(points), len(declared))
	}
	seen := make(map[string]bool, len(declared))
	for _, name := range declared {
		if !points[name] {
			t.Errorf("%q is declared but not registered in points", name)
		}
		if seen[name] {
			t.Errorf("%q is declared twice", name)
		}
		seen[name] = true
	}
}
