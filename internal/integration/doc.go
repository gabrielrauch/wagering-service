// Package integration holds the end-to-end suite: the whole service, driven
// over HTTP, against a real Keycloak and a real PostgreSQL.
//
// # Why it is a package of its own
//
// Every other suite in this tree belongs to the thing it tests. This one
// belongs to no single package, because what it tests is the seam between four
// of them: a token minted by an identity provider, verified by
// internal/adapters/oidc, turned into a principal the application layer
// authorises with, acting on rows internal/adapters/postgres wrote. Put in any
// one of those packages it would be that package's suite asserting things about
// its neighbours, and the first question of a failure — whose fault is this? —
// would already have been answered wrongly by where the test file happened to
// live.
//
// It is also the only suite here with nothing to reach for. The other
// integration suites are internal test packages because half of what they
// promise is a property of something unexported. This one has the opposite
// requirement: it may use only what a caller outside the process can use, which
// is a bearer token and an HTTP request, because anything else it touched would
// be a door the provider it is standing in for does not have.
//
// # What is real
//
// All of it. Keycloak 26.4 importing deploy/keycloak/realm-export.json, tokens
// obtained by a genuine client_credentials grant against that realm's token
// endpoint, PostgreSQL 16 with the eight migrations applied and the pool
// connected as wagering_app, the real oidc.Authenticator, the real httpapi.API
// over a real listener, and the real app.Wagering and app.Wallets over the real
// postgres adapter. Nothing here is a fake, a mock or an in-memory stand-in.
//
// The one thing this package signs for itself is a token no identity provider
// would issue: the forgery scenarios need a key the realm never published, and
// asking Keycloak for one is not a thing Keycloak does.
//
// This file carries no build tag so that the package exists for `go build` and
// `go vet` without one. Everything else here is behind `integration`.
package integration
