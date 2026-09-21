package app

import (
	"strings"

	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// PrincipalKind is the sort of identity an operation was submitted under.
type PrincipalKind string

const (
	// ProviderPrincipal is a game operator acting as itself. It may submit and
	// read its own operations and nothing else.
	ProviderPrincipal PrincipalKind = "PROVIDER"
	// ServicePrincipal is this system acting for itself: opening wallets,
	// reading them, reconciling them, and carrying parked work forward.
	ServicePrincipal PrincipalKind = "SERVICE"
)

// Principal is the authenticated identity an operation is submitted under.
//
// Its fields are unexported because a principal is an authorisation decision in
// value form: were they writable, any package could name any provider and the
// checks below would be answering a question the caller had already decided.
// The zero value authorises nothing, which is what an unconstructed one should
// do.
//
// Authorisation and existence are kept apart deliberately. Unauthorized is about
// the caller — it says the identity may not do this at all. NotFound is about the
// resource. A provider reading another provider's transaction is told NotFound
// rather than Unauthorized, because Unauthorized there would confirm that the
// transaction exists, and an existence oracle is exactly what scoping a read is
// meant to deny.
type Principal struct {
	kind     PrincipalKind
	provider wagering.Provider
	subject  string
}

// NewProviderPrincipal builds the identity of a provider acting as itself.
//
// The subject is who the token said they were, kept for the audit trail and
// never for a decision: every decision below is made from the provider.
//
// A principal that cannot be built is Unauthorized rather than Invalid, and
// names no field. Both values come from the token rather than from the payload,
// so Invalid would hand the caller a correctable code and point them at a
// request field that does not exist, for a request whose real problem is that
// nobody could tell who sent it.
func NewProviderPrincipal(provider wagering.Provider, subject string) (Principal, error) {
	if strings.TrimSpace(provider.String()) == "" {
		return Principal{}, unauthorized("a provider principal needs a provider")
	}
	if strings.TrimSpace(subject) == "" {
		return Principal{}, unauthorized("a principal needs a subject")
	}
	return Principal{kind: ProviderPrincipal, provider: provider, subject: subject}, nil
}

// NewServicePrincipal builds the identity of this system acting for itself.
func NewServicePrincipal(subject string) (Principal, error) {
	if strings.TrimSpace(subject) == "" {
		return Principal{}, unauthorized("a principal needs a subject")
	}
	return Principal{kind: ServicePrincipal, subject: subject}, nil
}

// Kind reports what sort of identity this is.
func (p Principal) Kind() PrincipalKind { return p.kind }

// Provider returns the provider a provider principal acts as, and false for
// anything else.
func (p Principal) Provider() (wagering.Provider, bool) {
	if p.kind != ProviderPrincipal {
		return "", false
	}
	return p.provider, true
}

// Subject returns who the token said the caller was.
func (p Principal) Subject() string { return p.subject }

// MaySubmitAs reports whether this principal may submit an operation naming
// provider.
//
// A provider may submit only as itself. The service does not submit wagers at
// all: openings are raised through Wallets.Open, which is a different door, and
// letting the service submit as any provider would make the check below
// meaningless for the one identity that could bypass it.
func (p Principal) MaySubmitAs(provider wagering.Provider) error {
	if p.kind != ProviderPrincipal {
		return unauthorized("%s may not submit wager operations", p.describe())
	}
	if p.provider != provider {
		return unauthorized("%s may not submit as %q", p.describe(), provider)
	}
	return nil
}

// MayAdministerWallets reports whether this principal may open, read, page or
// reconcile a wallet.
//
// Wallets are the service's to administer. A provider sees its own operations
// and the balance each one reported, which is everything it submitted and
// nothing it did not.
func (p Principal) MayAdministerWallets() error {
	if p.kind != ServicePrincipal {
		return unauthorized("%s may not administer wallets", p.describe())
	}
	return nil
}

// MayResume reports whether this principal may carry parked work forward.
//
// Resume is the worker's door. It is authorised by identity alone because it
// names no operation: what it will carry forward is whatever is due, and the
// authority to act on a given row comes from that row's own provider once it has
// been read, never from a claim made by the caller.
func (p Principal) MayResume() error {
	if p.kind != ServicePrincipal {
		return unauthorized("%s may not resume parked operations", p.describe())
	}
	return nil
}

// describe names the principal for a message without ever printing the subject,
// which comes from a token and does not belong in an error a provider will read.
func (p Principal) describe() string {
	switch p.kind {
	case ProviderPrincipal:
		return "provider " + string(p.provider)
	case ServicePrincipal:
		return "the service"
	default:
		return "an unauthenticated caller"
	}
}
