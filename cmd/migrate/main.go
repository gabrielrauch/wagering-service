// Command migrate applies and reverts the wagering schema.
//
// The migrations are embedded in this binary, so what it applies is what was
// built and tested — there is no directory to forget to ship alongside it.
//
// Usage:
//
//	migrate [-database URL] up
//	migrate [-database URL] down
//	migrate [-database URL] steps N
//	migrate [-database URL] version
//
// The database URL comes from -database, or from DATABASE_URL when the flag is
// absent. It must connect as a role that is a member of wagering_migrator; see
// docs/schema.md for which role each command uses.
//
// An interrupt stops the run at the next version boundary rather than in the
// middle of one, so a cancelled migration leaves the database at a version
// rather than dirty between two.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/gabrielrauch/wagering-service/internal/storage/postgres"
)

// Exit codes. Usage errors are told apart from failures so a script can
// distinguish "I called this wrongly" from "the migration did not apply".
const (
	exitOK = iota
	exitFailed
	exitUsage
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("migrate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	database := flags.String("database", os.Getenv("DATABASE_URL"),
		"PostgreSQL connection URL (defaults to DATABASE_URL)")
	flags.Usage = func() {
		_, _ = fmt.Fprint(stderr, `migrate applies and reverts the wagering schema.

Usage:
  migrate [-database URL] up        apply every migration not yet applied
  migrate [-database URL] down      revert every applied migration
  migrate [-database URL] steps N   apply N migrations, or revert -N of them
  migrate [-database URL] version   report the applied version

`)
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return exitUsage
	}

	rest := flags.Args()
	if len(rest) == 0 {
		flags.Usage()
		return exitUsage
	}
	if *database == "" {
		_, _ = fmt.Fprintln(stderr, "migrate: no database given; pass -database or set DATABASE_URL")
		return exitUsage
	}

	command, rest := rest[0], rest[1:]

	// Read the step count before opening anything, so that a miscalled command
	// is a usage error rather than a connection attempt.
	steps := 0
	if command == "steps" {
		if len(rest) != 1 {
			_, _ = fmt.Fprintln(stderr, "migrate: steps takes exactly one number")
			return exitUsage
		}
		n, err := strconv.Atoi(rest[0])
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "migrate: steps takes a number, got %q\n", rest[0])
			return exitUsage
		}
		steps = n
	}

	migrator, err := postgres.NewMigrator(*database)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "migrate: %v\n", err)
		return exitFailed
	}
	defer func() { _ = migrator.Close() }()

	switch command {
	case "up":
		err = migrator.Up(ctx)
	case "down":
		err = migrator.Down(ctx)
	case "steps":
		err = migrator.Steps(ctx, steps)
	case "version":
		// Nothing to run: the report below is the whole of this command.
	default:
		_, _ = fmt.Fprintf(stderr, "migrate: unknown command %q\n", command)
		flags.Usage()
		return exitUsage
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "migrate: %v\n", err)
		// Where the database landed matters as much as the failure: a run that
		// stopped part way is a different problem from one that never started.
		reportTo(ctx, migrator, stderr)
		return exitFailed
	}

	return report(ctx, migrator, stdout, stderr)
}

// report prints where the database now stands.
//
// A dirty database is reported as a failure rather than a version: a migration
// stopped part way, and the next thing anyone does should be to look at what it
// left behind rather than to run another one.
func report(ctx context.Context, migrator *postgres.Migrator, stdout, stderr io.Writer) int {
	version, dirty, err := migrator.Version(ctx)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "migrate: %v\n", err)
		return exitFailed
	}
	if dirty {
		_, _ = fmt.Fprintf(stderr, "migrate: version %d is dirty; a migration failed part way and needs inspecting\n", version)
		return exitFailed
	}
	_, _ = fmt.Fprintf(stdout, "%d\n", version)
	return exitOK
}

// reportTo says where a failed run left the database, on the same stream as the
// failure that prompted it.
func reportTo(ctx context.Context, migrator *postgres.Migrator, stderr io.Writer) {
	version, dirty, err := migrator.Version(ctx)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "migrate: the database version could not be read either: %v\n", err)
		return
	}
	if dirty {
		_, _ = fmt.Fprintf(stderr, "migrate: the database is left dirty at version %d\n", version)
		return
	}
	_, _ = fmt.Fprintf(stderr, "migrate: the database is left clean at version %d\n", version)
}
