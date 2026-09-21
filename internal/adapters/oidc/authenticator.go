package oidc

import (
	"context"
	"fmt"
	"strings"

	"github.com/lestrrat-go/jwx/v3/jws"

	"github.com/gabrielrauch/wagering-service/internal/app"
)

const (
	// bearerScheme is the only authentication scheme this service accepts.
	bearerScheme = "Bearer"
	// maxCredentialBytes bounds a credential before anything is decoded from
	// it. Eight kilobytes is what a common reverse proxy allows a single header
	// line to be, so a credential larger than this could not have reached a
	// production process through one — and refusing it by length is cheaper
	// than base64-decoding whatever it is instead.
	maxCredentialBytes = 8192
	// malformedToken is what a caller is told when the credential is not a JWS
	// at all. It says no more than the caller can see by looking at what they
	// sent.
	malformedToken = "the credential is not a well-formed token"
)

// Authenticator turns a bearer token into the [app.Principal] an operation is
// submitted under.
//
// Safe for concurrent use: everything it holds after construction is read-only
// but the key cache, which guards itself.
type Authenticator struct {
	settings settings
	keys     *keySet
}

// NewAuthenticator builds an authenticator from a checked configuration.
//
// It performs no I/O. A constructor that fetched the key set would make a
// misconfigured issuer a failure with nobody to report it to and no way to
// retry; [Authenticator.OnStart] is where reaching the issuer is allowed to
// fail, loudly and once.
func NewAuthenticator(cfg Config) (*Authenticator, error) {
	resolved, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	return &Authenticator{settings: resolved, keys: newKeySet(resolved)}, nil
}

// OnStart fetches the issuer's signing keys, bounded by the configured fetch
// timeout.
//
// It exists so that an issuer nobody can reach, a realm that does not exist and
// a JWKS document with nothing usable in it are startup failures rather than a
// wall of 401s at the first request. The error is the underlying one, rendered:
// its audience is whoever is starting the process, and the reticence the
// refusal path owes a caller buys nothing here.
func (a *Authenticator) OnStart(ctx context.Context) error {
	return a.keys.prime(ctx)
}

// Authenticate verifies a bearer credential and translates it into a principal.
//
// The order is the security content. The algorithm allow-list is applied to the
// JOSE header before any key is looked up, so a token naming an algorithm this
// service does not accept never reaches key selection — which is what closes
// the confusion attack, where a token signed with HMAC over the issuer's public
// key asks to be verified against that same public key. The claims are read
// only after the signature has been checked, because a claim from an unverified
// token is a claim an attacker wrote. The principal is built last, from the
// application layer's own constructors, so a token this package cannot place
// yields no principal rather than a permissive one.
//
// Every refusal classifies as [app.Unauthorized] and has no effect of any kind.
// The one error that is not a refusal is the caller's own context ending: a
// caller that gave up was not refused, and answering 401 to it would record an
// authentication failure that never happened.
func (a *Authenticator) Authenticate(ctx context.Context, bearer string) (app.Principal, error) {
	payload, err := a.verify(ctx, bearer)
	if err != nil {
		return app.Principal{}, err
	}

	claims, err := decodeClaims(payload)
	if err != nil {
		return app.Principal{}, err
	}
	if err := claims.validate(a.settings.clock.Now(), a.settings.issuer, a.settings.audience,
		a.settings.skew); err != nil {
		return app.Principal{}, err
	}
	return claims.principal()
}

// verify checks the signature and returns the payload it stands behind.
func (a *Authenticator) verify(ctx context.Context, bearer string) ([]byte, error) {
	bearer = strings.TrimSpace(bearer)
	switch {
	case bearer == "":
		return nil, refuse(ErrTokenRejected, "no credential was presented")
	case len(bearer) > maxCredentialBytes:
		return nil, refuse(ErrTokenRejected, malformedToken)
	case strings.Count(bearer, ".") != 2:
		// A bearer credential is the compact serialisation. jws.Parse also
		// reads the JSON one, which admits several signatures over a single
		// payload — and a verifier that accepted it would have to choose which
		// of them to believe. Refusing the shape means the choice never arises.
		return nil, refuse(ErrTokenRejected, malformedToken)
	}

	message, err := jws.Parse([]byte(bearer))
	if err != nil {
		return nil, refuse(ErrTokenRejected, malformedToken, err)
	}
	signatures := message.Signatures()
	if len(signatures) != 1 {
		return nil, refuse(ErrTokenRejected, malformedToken)
	}
	header := signatures[0].ProtectedHeaders()

	named, ok := header.Algorithm()
	if !ok {
		return nil, refuse(ErrTokenRejected, malformedToken)
	}
	// The allow-list, before a key is chosen. "none" and the HMAC family cannot
	// be in this table — [checkAlgorithms] refuses to build one containing them
	// — so both die here, at the header, with no key fetched and no fetch
	// triggered on their behalf.
	algorithm, allowed := a.settings.algorithms[named.String()]
	if !allowed {
		return nil, refuse(ErrTokenRejected,
			fmt.Sprintf("the token's %q algorithm is not accepted", named))
	}

	kid, ok := header.KeyID()
	if !ok || strings.TrimSpace(kid) == "" {
		// Selection here is by key identifier. A token without one would have
		// to be tried against every published key, which turns selection into
		// guessing and hands an attacker a way to make this process do work per
		// key; every issuer this service is built for sets it.
		return nil, refuse(ErrTokenRejected, "the token names no signing key")
	}

	key, err := a.keys.key(ctx, kid)
	if err != nil {
		if ctx.Err() != nil {
			return nil, app.AsRetryable(
				fmt.Errorf("oidc: obtain the issuer's signing keys: %w", err))
		}
		return nil, refuse(ErrKeyUnavailable, unverifiableSignature, err)
	}
	// A published key may name the one algorithm it is for, and Keycloak's do.
	// Honouring that is the same defence one level down: a key issued for
	// RS256 must not be talked into verifying a PS256 signature by a header
	// that asked nicely.
	if declared, ok := key.Algorithm(); ok && declared.String() != algorithm.String() {
		return nil, refuse(ErrTokenRejected, unverifiableSignature)
	}

	// The algorithm handed to the verifier is this package's own value, taken
	// from the allow-list table, rather than the one parsed out of the token.
	// They are equal by construction; passing the table's makes the data flow
	// say that the verifier chose it.
	payload, err := jws.Verify([]byte(bearer), jws.WithKey(algorithm, key))
	if err != nil {
		return nil, refuse(ErrTokenRejected, unverifiableSignature, err)
	}
	return payload, nil
}

// BearerToken reads the credential out of an Authorization header value.
//
// It lives here rather than in the HTTP adapter because the answer to a header
// that is missing, or carries another scheme, is the same 401 as the answer to
// a token that does not verify — and this package is where a refusal knows how
// to classify itself. The scheme is matched case-insensitively, as RFC 7235
// requires, and nothing else is accepted.
func BearerToken(header string) (string, error) {
	scheme, credential, found := strings.Cut(strings.TrimSpace(header), " ")
	credential = strings.TrimSpace(credential)
	if !found || !strings.EqualFold(scheme, bearerScheme) || credential == "" {
		return "", refuse(ErrTokenRejected, "no bearer credential was presented")
	}
	return credential, nil
}
