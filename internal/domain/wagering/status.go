package wagering

import (
	"slices"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// Status is where a wager transaction stands.
//
// # State machine
//
// An external operation is recorded as [Pending]. An opening is born
// [Processed], because a wallet's starting balance is applied as part of
// creating the wallet and there is no moment at which the opening is awaiting
// anything.
//
//	                     ┌──────────────┐
//	                     │  (external)  │
//	                     └──────┬───────┘
//	                            ▼
//	                       ┌─────────┐
//	       ┌───────────────┤ PENDING ├───────────────┐
//	       │               └────┬────┘               │
//	       │                    │                    │
//	       ▼                    ▼                    ▼
//	┌─────────────┐   ┌───────────────────┐   ┌──────────┐
//	│  PROCESSED  │   │ PENDING_REFERENCE │◀─┐│  FAILED  │
//	│ (terminal)  │   └─────────┬─────────┘  ││(terminal)│
//	└─────────────┘             │            │└──────────┘
//	       ▲                    └────────────┘
//	       │                     retry, attempt++
//	       │                          │
//	       └──────────────────────────┼──────────▶ ┌──────────┐
//	                                  └──────────▶ │ REJECTED │
//	                                               │(terminal)│
//	                                               └──────────┘
//
//	                     ┌──────────────┐
//	                     │  (opening)   │──────────▶ PROCESSED
//	                     └──────────────┘
//
// [Processed], [Rejected] and [Failed] are terminal: a transaction that has
// reached one never moves again, and an attempt to move it reports
// [failure.InvalidStateTransition] rather than panicking.
//
// Recording that a transaction has been reversed is not a status transition.
// Nothing about a reversed transaction changes — reversals are recorded by
// adding new transactions that point at it, never by writing to it.
type Status string

const (
	// Pending means the record was accepted and processing has not finished.
	Pending Status = "PENDING"
	// PendingReference means the operation is waiting for a reference that is
	// not available yet.
	PendingReference Status = "PENDING_REFERENCE"
	// Processed means the operation completed successfully. Terminal.
	Processed Status = "PROCESSED"
	// Rejected means a business rule refused the operation. Terminal.
	Rejected Status = "REJECTED"
	// Failed means a permanent infrastructure failure was recorded for audit.
	// Terminal.
	Failed Status = "FAILED"
)

// statuses lists every declared status.
var statuses = []Status{Pending, PendingReference, Processed, Rejected, Failed}

// transitions is the complete state machine. A status absent from a source's
// list cannot be reached from it, and the terminal statuses have no entry at
// all.
var transitions = map[Status][]Status{
	Pending:          {Processed, PendingReference, Rejected, Failed},
	PendingReference: {Processed, PendingReference, Rejected, Failed},
}

// Statuses returns every declared status.
func Statuses() []Status { return slices.Clone(statuses) }

// ParseStatus reads a status from its wire form.
func ParseStatus(s string) (Status, error) {
	if st := Status(s); st.Known() {
		return st, nil
	}
	return "", failure.New(failure.InvalidFieldFormat, "%q is not a known status", s).WithField("status")
}

// String returns the wire form of the status.
func (s Status) String() string { return string(s) }

// Known reports whether s is a declared status.
func (s Status) Known() bool { return slices.Contains(statuses, s) }

// IsTerminal reports whether the status is final.
func (s Status) IsTerminal() bool {
	return s == Processed || s == Rejected || s == Failed
}

// CanTransitionTo reports whether a transaction in status s may move to next.
func (s Status) CanTransitionTo(next Status) bool {
	return slices.Contains(transitions[s], next)
}
