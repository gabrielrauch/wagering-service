package httpapi

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"net/http"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
)

// decodeBody reads a request body into out, strictly.
//
// Strictly means four things, and encoding/json/v2 means all four without help:
// a member the endpoint does not have is refused rather than ignored, a member
// named twice is refused rather than resolved to the last one, a second value
// after the first is refused rather than discarded, and names match case for
// case. The first two are the ones that matter. An unknown member is usually a
// caller sending a field this service will silently not apply, and
// {"kind":"BET","kind":"LOSS"} decoded through the older package is a LOSS with
// nothing to say a BET was also asked for — which is the same class of problem
// money.UnmarshalJSON moved to this package to avoid.
//
// Nothing parsed here is a domain value. Every member lands in a string, and
// money lands in two, so that what a provider sent is what the application
// layer parses and hashes.
func decodeBody(r *http.Request, out any) error {
	err := json.UnmarshalRead(r.Body, out, json.RejectUnknownMembers(true))
	if err == nil {
		return nil
	}

	// The decoder's own message names the Go type it could not fill, which is
	// an internal name a caller has no business reading, so the reason is said
	// again here in the contract's terms.
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		return errTooLarge
	case errors.Is(err, json.ErrUnknownName):
		return malformedBody("names a field this endpoint does not have")
	case errors.Is(err, jsontext.ErrDuplicateName):
		return malformedBody("names the same field twice")
	default:
		return malformedBody("is not a JSON object this endpoint can read")
	}
}

// errTooLarge marks a body refused before it was read.
//
// It is a value rather than a message because the status it is answered with —
// 413 — has no application class behind it: nothing was submitted, so nothing
// was invalid, rejected or conflicting. [API.fail] recognises it by identity.
var errTooLarge = errors.New("httpapi: " + tooLargeMessage)

// tooLargeMessage is what the caller is told, without the package name an error
// value carries for the benefit of a log.
const tooLargeMessage = "the request body is larger than this service accepts"

// malformedBody refuses a body under the catalogue's own code for a field that
// is present but malformed, which is what a body this service cannot read is.
func malformedBody(why string) error {
	return failure.New(failure.InvalidFieldFormat, "the request body %s", why).WithField("body")
}
