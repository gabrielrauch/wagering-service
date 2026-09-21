// Package oidc turns a bearer token issued by the identity provider into the
// [app.Principal] an operation is submitted under, or into a classified
// refusal.
//
// It decides nothing about what a principal may do. Every authorisation
// question is answered by the methods on [app.Principal], in the application
// layer, from a value this package can only construct through the application's
// own constructors — so a token that names a role this package does not
// recognise produces no principal at all rather than a permissive one.
//
// # Why jwx rather than go-oidc
//
// The library is github.com/lestrrat-go/jwx/v3, and only its jwk and jws
// packages: a JWKS document is parsed by jwk, a signature is checked by jws,
// and the claims are decoded here. github.com/coreos/go-oidc would have been
// the shorter route and was rejected for two concrete reasons.
//
// The first is key selection. go-oidc's remote key set owns the cache and the
// refresh, and hands back a verified payload; there is no seam at which this
// package could refuse an algorithm BEFORE a key is chosen, and no seam at
// which an unknown "kid" could be rate-limited. Both of those are the whole
// security content of this package — see below — so a library that supplies
// them as an internal detail supplies them unverifiably.
//
// The second is construction. go-oidc's provider constructor performs discovery
// on the spot, which would make building an authenticator a network call and
// would put a failure to reach the identity provider in a place with nothing to
// report it to. Here construction is pure and [Authenticator.OnStart] is the
// bounded call that fails fast.
//
// Nothing in jwx's own caching is used. jwk.Cache would have brought a
// background refresh goroutine and a long-lived context, which is a lifecycle
// this package would then have to own and hand back; the cache in keys.go is
// perhaps sixty lines and the four properties that matter are testable in it.
//
// # What the verifier guarantees
//
//   - The algorithm allow-list is applied to the JOSE header before a key is
//     looked up, and the algorithm handed to the verifier is the one from this
//     package's own table rather than the one the token named. "none" and the
//     HMAC family are refused at construction, so an allow-list that admitted
//     them could not be built; the classic confusion attack — an HMAC token
//     signed with the issuer's public key — dies at the header, before the key
//     it is trying to be verified with is ever fetched.
//   - Only asymmetric public keys are ever cached. A JWKS entry of type "oct",
//     or one published for encryption rather than signing, is dropped at parse
//     time, so even a defect in the allow-list could not verify a token against
//     a shared secret. Keycloak publishes an encryption key alongside its
//     signing key, so that filter is load-bearing rather than theoretical.
//   - Issuer, audience, expiry, not-before and issued-at are all checked, after
//     the signature and never before it.
//   - Exactly one signature is accepted. A JWS may carry several over one
//     payload, and taking the first would let anybody append a signature of
//     their own to a token somebody else's key signed.
//   - Nothing on the key-fetching path follows a redirect. The origin checks
//     below test a URL, and an http.Client follows redirects by default, so
//     without that the checks would guard an address and not a destination.
//
// # What it does not guarantee: that a credential has one spelling
//
// The verifier is strict about what a token MEANS and lenient about how it is
// written down. Base64 padding, an embedded newline, and an extra member in a
// JSON serialisation are all tolerated by the parser, which reconstructs the
// signing input canonically — so one token can arrive as several distinct
// strings, all of which verify to the same claims. Re-serialising the header,
// adding a claim or changing the key identifier are refused, so no algorithm or
// key confusion is reachable through it; what is not provided is uniqueness of
// the bytes.
//
// That matters to exactly one kind of consumer: anything that remembers tokens
// — a replay cache, a denylist, a rate limit keyed on the credential. Nothing
// does today. Whatever adds one must key on a claim, "jti" being the obvious
// one, and not on the credential it arrived as.
//
// # What the key cache guarantees
//
//   - A token naming a "kid" the cache does not hold triggers one refresh, and
//     refreshes are rate-limited. Without the limit an attacker who can present
//     arbitrary key identifiers has a free amplification channel pointed at the
//     identity provider, paid for by this service's own reputation with it.
//   - Concurrent misses collapse into a single fetch. The fetch is not attached
//     to the context of whichever caller happened to start it, because the other
//     callers waiting on it did not ask to give up; it is bounded by its own
//     timeout instead, and each caller returns the moment its own context is
//     done.
//   - A refresh that fails leaves the cached keys alone. A momentary outage at
//     the identity provider must not turn into a wave of 401s for tokens this
//     process can still verify.
//
// # What a refusal says
//
// Every refusal classifies as [app.Unauthorized], so a handler maps it to 401
// without re-deriving anything, and none of them has a financial effect or
// reads a row.
//
// Two sentinels refine [ErrKeyUnavailable] rather than replacing it —
// [ErrUnknownKey] and [ErrRefreshDeclined] — because a stream of invented key
// identifiers and a rotation this process missed look identical from a caller
// and are not the same morning for an operator.
//
// The rendered message names a claim-level reason — expired, wrong issuer,
// wrong audience, disallowed algorithm — and that is deliberate: a JWT's header
// and payload are base64, not ciphertext, so whoever holds the token can
// already read every one of those facts and is told nothing new. What the
// message never distinguishes is a signature that did not check out from a key
// this process could not obtain. That difference is about the verifier's cache
// and the identity provider's availability, which the holder of a token cannot
// observe and must not be handed. It rides in the error chain instead, as
// [ErrTokenRejected] or [ErrKeyUnavailable], where [errors.Is] finds it and a
// rendered message does not — the shape [app.ErrForeignOperation] uses for the
// same problem.
//
// A token, a claim set and key material never appear in an error, a message or
// anything this package hands back.
//
// # The claims this reads
//
// Keycloak's client_credentials access token, and the parts of it that matter:
//
//   - "iss" must equal the configured issuer, compared byte for byte.
//   - "aud" must contain the configured audience, and may name others beside it.
//     Keycloak does not put an API's own identifier there by default, so the
//     realm has to say so with an audience mapper.
//   - "sub" is the service account's user id. It becomes the principal's
//     subject, which is audit trail and never a decision.
//   - "realm_access.roles" decides what sort of principal this is: "provider"
//     yields a provider principal, "internal" the service. See below.
//   - "providerId" is the hardcoded claim the provider clients carry, and is the
//     only place a provider's identity is read from.
//
// "azp" is present in every one of these tokens and is deliberately not read.
// It names the client that asked for the token, not the grant the token
// carries, and an identity decided by it would be an identity decided by which
// client happened to authenticate rather than by what the realm granted.
//
// # Why realm roles rather than client roles
//
// The role is read from "realm_access.roles" and from nowhere else.
// "resource_access" is keyed by client id, so reading a role out of it means
// choosing the bucket with "azp" — which puts the identity decision back on the
// claim that was just rejected for making it. A realm role is a grant the realm
// made to this service account; it is one array, in one place, and moving it
// takes a deliberate change to the realm rather than a rename of a client.
//
// A token carrying both roles, neither role, or "provider" without a usable
// "providerId" is refused. Resolving it to one of the two would make this
// package pick which authorisation boundary applies, and that pick is exactly
// the decision the application layer keeps for itself.
package oidc
