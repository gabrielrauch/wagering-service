//go:build multi

// Starting a real binary, reading what it said, and asserting on how it died.
//
// This is the part of the suite nothing else in the tree has. The other
// integration suites build the loops in process and call Start and Stop on
// them; these scenarios are about what survives a process that does not get to
// run its Stop, so the loops have to be somewhere a kill can reach.
package multi

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// entry is one line a process wrote.
//
// Both forms are kept. The service logs JSON, which is what an assertion reads;
// internal/faults writes one plain sentence to standard error before it exits,
// which is what proves the death was asked for — and a journal that only kept
// the parsed form would lose exactly that line.
type entry struct {
	raw    string
	fields map[string]any
}

// message is the line's "msg", which is how a log line is identified here.
func (e entry) message() string { return e.text("msg") }

// text reads a string attribute, answering "" for one that is absent or is not
// a string.
func (e entry) text(name string) string {
	value, _ := e.fields[name].(string)
	return value
}

// number reads a numeric attribute. JSON has one number type, so an integer
// attribute arrives as a float and is compared as one.
func (e entry) number(name string) (float64, bool) {
	value, ok := e.fields[name].(float64)
	return value, ok
}

// flag reads a boolean attribute, and says whether it was there — because
// `replay` being false and `replay` being absent are different findings.
func (e entry) flag(name string) (bool, bool) {
	value, ok := e.fields[name].(bool)
	return value, ok
}

// journal is everything a process has written so far.
//
// One journal per process, fed by two writers — standard output and standard
// error — each with its own partial-line buffer, so that two descriptors
// writing at once cannot splice one line into another.
type journal struct {
	mu   sync.Mutex
	seen []entry
}

// writer hands out a descriptor's end of this journal.
func (j *journal) writer() io.Writer { return &journalWriter{journal: j} }

// journalWriter is one descriptor writing into a [journal].
type journalWriter struct {
	journal *journal
	rest    []byte
}

// Write splits what arrived into lines, keeping anything after the last
// newline for the write that completes it.
func (w *journalWriter) Write(p []byte) (int, error) {
	w.rest = append(w.rest, p...)
	for {
		at := bytes.IndexByte(w.rest, '\n')
		if at < 0 {
			return len(p), nil
		}
		line := string(bytes.TrimRight(w.rest[:at], "\r"))
		w.rest = w.rest[at+1:]
		if strings.TrimSpace(line) != "" {
			w.journal.add(line)
		}
	}
}

// add records one line, parsing it as a log entry when it is one.
func (j *journal) add(line string) {
	parsed := entry{raw: line}
	var fields map[string]any
	if err := json.Unmarshal([]byte(line), &fields); err == nil {
		parsed.fields = fields
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.seen = append(j.seen, parsed)
}

// entries is a copy of everything written so far.
func (j *journal) entries() []entry {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]entry(nil), j.seen...)
}

// matching is every line the predicate accepts.
func (j *journal) matching(match func(entry) bool) []entry {
	var found []entry
	for _, e := range j.entries() {
		if match(e) {
			found = append(found, e)
		}
	}
	return found
}

// String renders the journal for a failure message, newest last.
func (j *journal) String() string {
	var out strings.Builder
	for _, e := range j.entries() {
		out.WriteString("    ")
		out.WriteString(e.raw)
		out.WriteByte('\n')
	}
	if out.Len() == 0 {
		return "    (it wrote nothing)"
	}
	return strings.TrimRight(out.String(), "\n")
}

// process is one real binary this suite started.
type process struct {
	// name is what failures call it: "worker-armed", "publisher-one". It is
	// this suite's name for the process and not the process's own.
	name string
	cmd  *exec.Cmd
	log  *journal
	// done is closed when the process has been reaped, after which [process.err]
	// may be read.
	done chan struct{}
	err  error
	// stopped guards against signalling or reaping twice, which t.Cleanup plus
	// an explicit stop would otherwise do.
	stopped sync.Once
}

// launch starts a binary with exactly the environment given, plus PATH and HOME.
//
// Exactly, rather than on top of the test process's own: the host that runs
// this suite may well have DATABASE_URL or FAULT_POINT set for its own
// purposes, and a process that inherited either would be configured by
// something no test can see.
func launch(t *testing.T, name, binary string, env map[string]string) *process {
	t.Helper()
	// CommandContext with a context that is never cancelled, rather than
	// Command: this process outlives the call that starts it by design, and
	// killing it is [process.terminate]'s job and nobody else's.
	cmd := exec.CommandContext(context.Background(), binary)
	cmd.Env = environment(env)
	log := &journal{}
	cmd.Stdout = log.writer()
	cmd.Stderr = log.writer()

	p := &process{name: name, cmd: cmd, log: log, done: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s (%s): %v", name, binary, err)
	}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	// Registered before anything a test does with the process, so that it runs
	// after: cleanups run last in, first out, and a process left running would
	// go on claiming outbox rows while the next test read them.
	t.Cleanup(func() { p.terminate() })
	return p
}

// environment renders the child's environment, with the two variables that are
// about the machine rather than about the service carried over.
func environment(values map[string]string) []string {
	out := make([]string, 0, len(values)+2)
	for _, name := range []string{"PATH", "HOME"} {
		if value, set := os.LookupEnv(name); set {
			out = append(out, name+"="+value)
		}
	}
	for name, value := range values {
		out = append(out, name+"="+value)
	}
	return out
}

// awaitExit waits for the process to end on its own and answers the status it
// ended with.
//
// A process that is still running when the budget runs out is a failure and not
// a zero: every caller of this is asserting that something killed it.
func (p *process) awaitExit(t *testing.T, budget time.Duration) int {
	t.Helper()
	select {
	case <-p.done:
		return statusOf(p.err)
	case <-time.After(budget):
		t.Fatalf("%s was still running after %s, and was expected to have exited\n%s",
			p.name, budget, p.log)
		return 0
	}
}

// exited reports whether the process has already ended.
func (p *process) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// stop asks the process to shut down the way a deployment does, and insists it
// goes cleanly.
//
// SIGTERM rather than a kill, because what this asserts is the drain: a worker
// stopped this way hands back the messages and the outbox claims it holds, and
// a scenario that killed it instead would be leaving work behind for the next
// assertion to trip over.
func (p *process) stop(t *testing.T) {
	t.Helper()
	if p.exited() {
		return
	}
	p.terminate()
	select {
	case <-p.done:
	case <-time.After(shutdownBudget):
		t.Fatalf("%s did not stop inside %s\n%s", p.name, shutdownBudget, p.log)
		return
	}
	if status := statusOf(p.err); status != 0 {
		t.Errorf("%s exited %d on a clean shutdown, wanted 0\n%s", p.name, status, p.log)
	}
}

// terminate signals the process once and, failing that, kills it. It is what
// t.Cleanup runs and what [process.stop] asks first.
func (p *process) terminate() {
	p.stopped.Do(func() {
		if p.exited() {
			return
		}
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			_ = p.cmd.Process.Kill()
			return
		}
		select {
		case <-p.done:
		case <-time.After(shutdownBudget):
			_ = p.cmd.Process.Kill()
		}
	})
}

// awaitLine waits for this process to write a line the predicate accepts.
//
// It gives up the moment the process has exited without writing one, because
// the reason it exited is in what it did write and waiting the whole budget out
// to report "it never said that" would bury it. [journal.await] is the same
// wait for a process that is expected to outlive it.
func (p *process) awaitLine(
	t *testing.T, budget time.Duration, what string, match func(entry) bool,
) entry {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		if found := p.log.matching(match); len(found) > 0 {
			return found[0]
		}
		if p.exited() {
			t.Fatalf("%s exited %d without %s\n%s", p.name, statusOf(p.err), what, p.log)
			return entry{}
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited %s for %s and it never happened\n%s", budget, what, p.log)
			return entry{}
		}
		time.Sleep(pollInterval)
	}
}

// statusOf is the exit status behind what [exec.Cmd.Wait] reported.
//
// A process killed by a signal reports 128+n here, which is the shell's
// convention and not Go's — Go's own ExitCode answers -1 for a signal, and -1
// beside internal/faults.ExitCode would make "it was killed" and "it exited
// with a status nobody recognises" the same finding.
func statusOf(err error) int {
	if err == nil {
		return 0
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		if status, ok := exit.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return 128 + int(status.Signal())
		}
		return exit.ExitCode()
	}
	return -1
}

// diedAtFaultPoint asserts that a process was killed by the fault point named,
// and by nothing else.
//
// Two things are checked and both are necessary. The status says a fault fired
// rather than that the process crashed — internal/faults.ExitCode is 99 for
// exactly this reason — and the line it wrote first says WHICH one, because a
// scenario that armed the wrong point would otherwise pass on the strength of a
// death it did not arrange.
func diedAtFaultPoint(t *testing.T, p *process, point string, budget time.Duration) {
	t.Helper()
	const faultExitCode = 99
	if status := p.awaitExit(t, budget); status != faultExitCode {
		t.Fatalf("%s exited %d, wanted %d — the fault point %s did not fire\n%s",
			p.name, status, faultExitCode, point, p.log)
	}
	want := fmt.Sprintf("FAULT_POINT=%s fired", point)
	for _, e := range p.log.entries() {
		if strings.Contains(e.raw, want) {
			return
		}
	}
	t.Fatalf("%s exited %d but never said %q, so it died at some other point\n%s",
		p.name, faultExitCode, want, p.log)
}
