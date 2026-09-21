package app

import (
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/wagering"
)

// The ledger cursor is a wallet and a version, base64url encoded.
//
// It is opaque so that the pagination scheme is not part of the external
// contract: a caller that learned to read "wallet:version" would be depending on
// it, and changing how a page is found would then be a breaking change.
//
// It names the wallet as well as the position, which is what lets a cursor from
// one wallet be refused on another. Without it, a cursor is just a number, and
// passing the wrong one silently returns a page of somebody else's ledger.
//
// The position is a wallet version rather than an offset or a timestamp. Two
// entries can share a creation instant, and an offset shifts under an insert,
// but (walletId, walletVersion) is unique and ordered, so a page boundary drawn
// on it cannot skip or repeat an entry.

func encodeCursor(w wagering.WalletID, version uint64) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(w.String() + ":" + strconv.FormatUint(version, 10)))
}

// decodeCursor reads a cursor, refusing one that does not belong to the wallet
// being paged.
func decodeCursor(cursor string, w wagering.WalletID) (uint64, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, invalidField("cursor", failure.InvalidFieldFormat, "cursor is not valid base64url")
	}
	id, version, found := strings.Cut(string(raw), ":")
	if !found {
		return 0, invalidField("cursor", failure.InvalidFieldFormat, "cursor is malformed")
	}
	if id != w.String() {
		return 0, invalidField("cursor", failure.InvalidFieldFormat,
			"cursor belongs to another wallet")
	}
	after, err := strconv.ParseUint(version, 10, 64)
	if err != nil {
		return 0, invalidField("cursor", failure.InvalidFieldFormat, "cursor position is not a version")
	}
	return after, nil
}
