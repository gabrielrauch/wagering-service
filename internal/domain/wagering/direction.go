package wagering

import (
	"github.com/gabrielrauch/wagering-service/internal/domain/failure"
	"github.com/gabrielrauch/wagering-service/internal/domain/money"
)

// Direction is the way a ledger entry moved money.
type Direction string

const (
	// Debit took money out of the wallet.
	Debit Direction = "DEBIT"
	// Credit put money into the wallet.
	Credit Direction = "CREDIT"
)

// ParseDirection reads a direction from its wire form.
func ParseDirection(s string) (Direction, error) {
	if d := Direction(s); d.Known() {
		return d, nil
	}
	return "", unknownDirection(Direction(s))
}

// String returns the wire form of the direction.
func (d Direction) String() string { return string(d) }

// Known reports whether d is a declared direction.
func (d Direction) Known() bool { return d == Debit || d == Credit }

// unknownDirection is the single refusal for a direction that names no
// movement, so every place that rejects one says the same thing.
func unknownDirection(d Direction) *failure.Error {
	return failure.New(failure.InvalidFieldFormat, "%q is not a known direction", d).WithField("direction")
}

// Opposite returns the direction that undoes d.
func (d Direction) Opposite() (Direction, bool) {
	switch d {
	case Debit:
		return Credit, true
	case Credit:
		return Debit, true
	default:
		return "", false
	}
}

// applyDirection moves amount against balance the way direction says.
func applyDirection(d Direction, balance, amount money.Money) (money.Money, error) {
	switch d {
	case Credit:
		return balance.Add(amount)
	case Debit:
		return balance.Sub(amount)
	default:
		return money.Money{}, unknownDirection(d)
	}
}
