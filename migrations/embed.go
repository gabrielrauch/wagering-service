// Package migrations carries the versioned SQL that defines the wagering
// schema.
//
// The files live at the repository root rather than beside the code that runs
// them, because they are the artefact an operator reaches for first and because
// [embed] cannot reach outside its own package directory. This package exists
// to give that directory a package to be embedded by.
//
// Each version is a pair — NNNNNN_name.up.sql and NNNNNN_name.down.sql — and
// every up has a down that reverses everything the migration owns in this
// database. What it deliberately leaves standing is cluster-wide and shared, so
// no one database's revert may decide it is finished with; 000001 and ADR-0009
// argue that case for the two roles. See docs/schema.md for what each version owns and how
// to apply and revert them.
package migrations

import "embed"

// FS holds every migration file, in the layout golang-migrate's iofs source
// expects.
//
//go:embed *.sql
var FS embed.FS
