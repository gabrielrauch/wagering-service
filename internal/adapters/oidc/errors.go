package oidc

import (
	"errors"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

// Why a credential was refused, told to an operator and never to a caller.
//
// Each rides in a refusal's chain where [errors.Is] finds it, and none of them
// appears in the message the refusal renders — the shape
// [app.ErrForeignOperation] uses, and for the same reason. The pair that must
// stay indistinguishable on the outside is [ErrTokenRejected] and
// [ErrKeyUnavailable]: a signature that did not check out and a key this
// process could not obtain are one answer to whoever presented the token, and
// two entirely different mornings for whoever runs the service.
//
// # What a handler must not do with them
//
// The same warning [app.ErrForeignOperation] carries. An adapter that renders
// an error chain to the caller — %+v, a cause list in a JSON body, an unwrapped
// detail field — publishes the one fact these exist to keep private. Match
// them, count them, log them; never answer with them.
var (
	// ErrTokenRejected marks a credential this service examined and would not
	// accept: malformed, wrongly signed, expired, or addressed elsewhere.
	ErrTokenRejected = errors.New("oidc: token rejected")
	// ErrKeyUnavailable marks a credential this service could not examine,
	// because the key it names is not one the issuer's published set could be
	// shown to contain.
	ErrKeyUnavailable = errors.New("oidc: no usable signing key")
	// ErrPrincipalUnresolved marks a token that verified and whose claims name
	// no identity this system recognises. It is the one refusal that usually
	// means the realm is wrong rather than the caller.
	ErrPrincipalUnresolved = errors.New("oidc: token names no principal")
)

// Why a key was unavailable. Both ride alongside [ErrKeyUnavailable] rather
// than instead of it, so a handler that only asks the question above still gets
// the same answer.
//
// They are apart because they are two different mornings as well. A steady
// stream of [ErrUnknownKey] is somebody presenting invented key identifiers; a
// steady stream of [ErrRefreshDeclined] is the rate limit doing its job while
// that happens, or — if it outlasts the interval — a rotation this process
// missed. A count of each is the cheapest way to tell the two apart, and the
// same warning applies: never answer with them.
var (
	// ErrUnknownKey is a key identifier the issuer's published set does not
	// contain, after a refresh that was allowed to happen.
	ErrUnknownKey = errors.New("oidc: unknown key identifier")
	// ErrRefreshDeclined is a refresh the rate limit turned away. It is what an
	// attacker presenting a stream of invented key identifiers gets, and it
	// costs the identity provider nothing.
	ErrRefreshDeclined = errors.New("oidc: the key set was refreshed too recently")
)

// unverifiableSignature is what a caller is told whenever the signature did not
// check out, whichever of the two reasons it was. See the block above.
const unverifiableSignature = "the token's signature could not be verified"

// refusal is a credential this package would not accept.
//
// It renders the reason and nothing else: not the credential, not the claim
// set, not the key, and not the causes. The classification and the operator's
// distinction both ride in the chain — [app.ClassOf] finds the Unauthorized
// class through it and [errors.Is] finds the sentinel — so a handler answers
// 401 without re-deriving anything, and the answer never varies with which of
// the two went wrong.
type refusal struct {
	reason string
	causes []error
}

// Error renders the reason, prefixed with the package that refused.
func (r *refusal) Error() string { return "oidc: " + r.reason }

// Unwrap exposes the causes, which is what keeps [errors.Is] finding the
// sentinel and [app.ClassOf] finding the class.
func (r *refusal) Unwrap() []error { return r.causes }

// refuse builds a refusal: the sentinel an operator matches, the reason a
// caller may be shown, and whatever cause carried the detail.
//
// The [app.Error] is built fresh each time rather than shared from a
// package-level value. It is a pointer to a struct with exported fields, and
// one instance reachable from every refusal in the process is one instance
// anybody could reclassify for all of them.
func refuse(why error, reason string, causes ...error) error {
	all := make([]error, 0, len(causes)+2)
	all = append(all, why)
	all = append(all, causes...)
	return &refusal{reason: reason, causes: append(all, &app.Error{Class: app.Unauthorized})}
}
