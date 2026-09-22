package oidc

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gabrielrauch/wagering-service/internal/app"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The realm roles this service recognises, and the only two identities it has.
//
// They are read from "realm_access.roles" and from nowhere else — see the
// package doc for why a realm role rather than a client role, and why never
// "azp".
const (
	// providerRoleName is granted to a game operator's service account.
	providerRoleName = "provider"
	// internalRoleName is granted to this system's own service account.
	internalRoleName = "internal"
)

// providerIDClaim is the hardcoded claim a provider client's tokens carry. It
// is the only place a provider's identity is read from.
const providerIDClaim = "providerId"

// claims is the part of an access token this package reads.
//
// Everything else the identity provider puts in a token is ignored, and the
// decoder is deliberately not strict about unknown fields: a realm adds claims
// over its lifetime, and a verifier that refused a token for carrying one it
// had not heard of would fail closed on a change that concerns it not at all.
//
// "azp" is absent by choice rather than by oversight. It names the client that
// asked for the token; the roles name what the realm granted, and only the
// second is a grant.
type claims struct {
	Issuer      string   `json:"iss"`
	Subject     string   `json:"sub"`
	Audience    audience `json:"aud"`
	ExpiresAt   *int64   `json:"exp"`
	NotBefore   *int64   `json:"nbf"`
	IssuedAt    *int64   `json:"iat"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
	ProviderID string `json:"providerId"`
}

// audience decodes the "aud" claim, which RFC 7519 allows to be either one
// string or an array of them. Both spellings arrive in practice — Keycloak
// emits the first when a token has a single audience — so both are read, and
// neither is treated as more authoritative than the other.
type audience []string

// UnmarshalJSON implements [json.Unmarshaler].
func (a *audience) UnmarshalJSON(data []byte) error {
	var one string
	if err := json.Unmarshal(data, &one); err == nil {
		*a = audience{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return fmt.Errorf("the aud claim is neither a string nor an array of them: %w", err)
	}
	*a = audience(many)
	return nil
}

// decodeClaims reads a verified payload.
//
// The numeric dates are int64 rather than float64, so a NumericDate carrying a
// fraction is refused as malformed instead of quietly rounded. RFC 7519 permits
// the fraction; nothing that issues a token to this service emits one, and
// accepting it would mean this package choosing which direction to round an
// expiry — a choice with a right answer in only one of the two directions and
// no way to tell which from here.
func decodeClaims(payload []byte) (claims, error) {
	var c claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return claims{}, refuse(ErrTokenRejected, "the token's claims are not readable", err)
	}
	return c, nil
}

// validate holds a verified token to the four things a signature does not say:
// who issued it, who it is for, and the window it is good in.
//
// Every one of these is checked after the signature and never before it. A
// claim read out of an unverified token is a claim an attacker wrote.
func (c claims) validate(now time.Time, issuer, wanted string, skew time.Duration) error {
	if c.Issuer != issuer {
		return refuse(ErrTokenRejected, "the token was issued by another issuer")
	}
	if !slices.Contains(c.Audience, wanted) {
		return refuse(ErrTokenRejected, "the token is not addressed to this service")
	}
	if strings.TrimSpace(c.Subject) == "" {
		return refuse(ErrTokenRejected, "the token names no subject")
	}

	// An expiry is required. A token without one never stops working, and a
	// verifier that accepted it would have turned a credential into a password.
	if c.ExpiresAt == nil {
		return refuse(ErrTokenRejected, "the token states no expiry")
	}
	if now.After(time.Unix(*c.ExpiresAt, 0).Add(skew)) {
		return refuse(ErrTokenRejected, "the token has expired")
	}
	// Not-before and issued-at are optional, because a token is allowed not to
	// state them. When one is stated it is honoured: a token that says it is not
	// valid yet is not valid yet, whatever else it says.
	if c.NotBefore != nil && now.Before(time.Unix(*c.NotBefore, 0).Add(-skew)) {
		return refuse(ErrTokenRejected, "the token is not valid yet")
	}
	if c.IssuedAt != nil && now.Before(time.Unix(*c.IssuedAt, 0).Add(-skew)) {
		return refuse(ErrTokenRejected, "the token was issued in the future")
	}
	return nil
}

// principal translates a verified, valid token into the identity it was
// submitted under.
//
// The role decides, and it decides alone. The three refusals below are the
// point of the function: a token that carries both roles, neither, or the
// provider role without a provider is not resolved to whichever reading would
// let it through, because choosing between them is an authorisation decision
// and this package does not make those.
func (c claims) principal() (app.Principal, error) {
	isProvider := slices.Contains(c.RealmAccess.Roles, providerRoleName)
	isInternal := slices.Contains(c.RealmAccess.Roles, internalRoleName)

	switch {
	case isProvider && isInternal:
		// Two roles are two authorisation boundaries, and they are different
		// ones: a provider submits operations and may not touch a wallet, the
		// service administers wallets and may not submit. Picking one would be
		// this package deciding which of the realm's two grants it meant.
		return app.Principal{}, refuse(ErrPrincipalUnresolved,
			"the token carries both the provider and the service role")
	case isProvider:
		provider, err := wagering.NewProvider(c.ProviderID)
		if err != nil {
			// Deliberately not falling back to "azp". A provider whose
			// hardcoded claim is missing is a realm that was built wrongly, and
			// inferring the provider from the client that authenticated would
			// hide that by guessing — correctly, right up until the day two
			// clients belong to one provider or one client is renamed.
			return app.Principal{}, refuse(ErrPrincipalUnresolved,
				"the provider role carries no usable "+providerIDClaim+" claim", err)
		}
		principal, err := app.NewProviderPrincipal(provider, c.Subject)
		if err != nil {
			return app.Principal{}, refuse(ErrPrincipalUnresolved,
				"the token names no provider principal", err)
		}
		return principal, nil
	case isInternal:
		principal, err := app.NewServicePrincipal(c.Subject)
		if err != nil {
			return app.Principal{}, refuse(ErrPrincipalUnresolved,
				"the token names no service principal", err)
		}
		return principal, nil
	default:
		return app.Principal{}, refuse(ErrPrincipalUnresolved,
			"the token carries neither the provider nor the service role")
	}
}
