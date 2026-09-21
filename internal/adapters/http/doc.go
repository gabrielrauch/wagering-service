// Package httpapi is the HTTP side of the application's two services: the
// router, the handlers, the request parsing, the response mapping, the
// correlation that ties a request to everything done for it, and the server
// that carries them.
//
// The directory is named http because that is what this adapter is; the package
// is not, because a package named http would have to alias net/http in every
// file, and "stdhttp.StatusOK" beside "http.StatusOK" in a package whose whole
// subject is HTTP is a rename waiting to be got wrong.
//
// # It parses nothing a provider sent
//
// Every field of a submission crosses this package as the bytes it arrived as
// and is handed to [app.OperationFields] unchanged — no trimming, no
// upper-casing, no re-rendering of an amount. The application layer parses a
// submission exactly once so that HTTP and SQS cannot disagree about what it
// said, and the idempotency hash is taken over those parsed values: a value
// repaired on the way in would be hashed as something the provider never sent,
// and two transports repairing differently would disagree about whether a
// submission is a retry.
//
// The same rule is why the request bodies here decode into structs of strings.
// Nothing in this package calls money.Parse, and money.Money has no
// UnmarshalJSON call site on the request path at all.
//
// # The Idempotency-Key is never computed
//
// It is read from the header and nowhere else, and a request without one is
// refused. Deriving a key from the body would make every distinct payload its
// own key, which is the opposite of what the header is for: two submissions
// that differ by a field a provider did not mean to change would then both be
// applied, and the second would not be recognised as the retry it was.
//
// # What is never written to a response
//
// An error's cause chain, in any form: no %+v, no cause list, no unwrapped
// detail member. [app.ErrForeignOperation] and the refusals in
// internal/adapters/oidc both keep a distinction an operator needs inside the
// error chain precisely because a rendered message does not show it — a
// provider reading another provider's operation is told exactly what a caller
// asking for something absent is told, byte for byte, and a credential this
// service could not verify is told exactly what one it verified and rejected is
// told. Rendering the chain would publish both distinctions and turn a scoped
// read back into the existence oracle scoping it was meant to deny.
//
// The only thing ever rendered as a message is an [app.Error]'s or a
// [failure.Error]'s own words, and only for the classes a caller can act on.
// Everything else answers with a fixed sentence and logs the rest.
//
// # 401 and 403
//
// [app.Unauthorized] covers both "nobody could tell who you are" and "we know
// who you are and you may not do this", and the application layer cannot tell
// them apart — it never sees a request that failed to authenticate. This
// package can, because authentication happens here: a credential refused by the
// authenticator is 401, and a refusal raised by an [app.Principal] that was
// successfully built is 403.
//
// # Where authorisation is decided
//
// Almost nowhere here. A provider may not administer wallets because
// [app.Principal.MayAdministerWallets] says so, and may not submit as somebody
// else because [app.Principal.MaySubmitAs] says so; this package calls the
// service and maps what comes back. The one check it makes for itself is that
// the provider named in a path is the provider the token names, because the
// application layer never sees that path segment.
//
// # The router
//
// net/http's ServeMux, with method-and-wildcard patterns. It matches the
// routes this API has exactly, so a third-party router would be a dependency
// bought for nothing. Its own two refusals — no route, and a route that exists
// under another method — are the only responses this package does not compose
// itself, so they are intercepted and restated in the contract's shape rather
// than left as the two lines of plain text the mux writes.
package httpapi
