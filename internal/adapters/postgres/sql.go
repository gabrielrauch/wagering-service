package postgres

import (
	"strconv"
	"strings"
)

// The statement text this package assembles, and the three things it assembles
// it from.
//
// A projection is a slice of column names rather than a comma-separated string,
// and that is the whole point of this file. Qualifying a projection for a join
// used to mean splitting a string on ", " and putting it back together, which
// worked only for as long as every projection happened to be written with
// exactly that separator — a reformatting away from producing silent nonsense.
// Names in, names out; the separator is written once, here.
//
// The placeholder list is generated from the same slice for the same reason.
// An insert's columns and its $1..$n were two hand-maintained lists that had to
// agree, and the first time they did not, a timestamp was written into the
// column after the one it belonged to and the row was refused three statements
// later for a NOT NULL nobody had touched.

// columns renders a projection for a SELECT or an INSERT column list.
func columns(names []string) string { return strings.Join(names, ", ") }

// qualified renders a projection with a table alias in front of every name, for
// the one query that reads the same shape twice.
func qualified(alias string, names []string) string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = alias + "." + name
	}
	return columns(out)
}

// placeholders renders $1 .. $n, so that an insert's parameter list is derived
// from its column list rather than counted alongside it.
func placeholders(n int) string {
	out := make([]string, n)
	for i := range out {
		out[i] = "$" + strconv.Itoa(i+1)
	}
	return columns(out)
}

// insertInto renders a complete INSERT with one placeholder per column.
func insertInto(table string, names []string) string {
	return "INSERT INTO " + table + " (" + columns(names) + ") VALUES (" +
		placeholders(len(names)) + ")"
}
