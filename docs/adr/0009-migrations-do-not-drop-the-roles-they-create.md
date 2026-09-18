# Migrations create the roles but never drop them

`000001` creates `wagering_migrator` and `wagering_app` idempotently. Its down migration
revokes what they hold in this database and **leaves both roles standing**. Nothing in
`migrations/` ever runs `DROP ROLE`, and nothing runs `DROP OWNED BY`.

## Why this needs recording

Every other version in this schema is an exact inverse of itself, and the up/down/up test
exists to prove it. A reader who finds `CREATE ROLE` in `000001.up.sql` with no `DROP ROLE`
in `000001.down.sql` will read it as the one place the discipline slipped, and will be
tempted to "finish" the down migration.

It is not an oversight. The symmetric version was written first, it passed its tests, and it
was wrong in a way the tests had been arranged not to see.

## What goes wrong when a migration drops them

**Roles are cluster-wide; a migration is per-database.** A revert therefore cannot know
whether another database on the same cluster is still using the roles — and PostgreSQL will
not let it find out. `DROP OWNED BY` reaches only the current database, so `DROP ROLE` raises
`2BP01` the moment a sibling database still holds a grant written against that role.

That failure lands **mid-migration**. The database being reverted is left recorded at version
`-1` and dirty, with its objects already dropped: unrecoverable without a manual force, and
caused entirely by a database it has nothing to do with. The blast radius of reverting one
service's database is every other database on the cluster that shares the role name.

**`DROP OWNED BY` is also unrunnable by the login that deploys.** It requires the caller to
hold the privileges of the role it names. The migrating login is documented as a member of
`wagering_migrator` and nothing else, so `DROP OWNED BY wagering_app` raises `42501` before
any of the above is even reached.

The first version of these migrations hid both problems by giving the lifecycle test a
cluster of its own. That was the test being shaped to fit the migration rather than the other
way round: the one scenario it could not observe was the one that breaks in production, where
a cluster hosts more than one database.

## What we considered

**Drop them, and accept the constraint that a cluster hosts one database.** Rejected: it is
not a constraint anyone stated, it is invisible until it fires, and it fires during a revert —
when the person running it is already having a bad day.

**Drop them only when nothing else depends on them.** `pg_shdepend` *is* cluster-wide, so the
check is possible. It was still rejected: the answer is racy — another database can acquire a
grant between the check and the drop — and even a true answer leaves nothing useful to do,
because the dependencies in other databases cannot be cleared from this one. It buys a
narrower failure window, not a correct outcome.

**Leave them standing.** The up creates both roles idempotently, catching `duplicate_object`
and `unique_violation`, *precisely so that finding them already there is normal*. A revert
that leaves them is therefore not leaving debris — it is leaving the state the next apply
expects.

## Consequences

- A full revert leaves the two roles holding **nothing in this database**. `000008` revokes
  the application's grants; `000001` revokes the migration role's and hands the schema's
  ownership back to whoever is reverting. What survives is a name and a membership list.
- The rule the down migrations actually follow is narrower than "every up has an exact down":
  every up has a down that reverses everything the migration owns **in this database**. What
  it leaves standing is cluster-wide and shared, so no one database's revert may decide it is
  finished with it.
- Role lifetime belongs to deployment, alongside the login users and the credentials that are
  already kept out of migrations for the same reason.
- Removing the roles for real is an operator task, not a migration: `DROP OWNED BY` in each
  database that used them, then `DROP ROLE`. Deliberate, and outside any single database's
  revert.
- `assertSchemaIsClean` asserts both roles are still standing after a full revert, so
  re-introducing `DROP ROLE` fails the lifecycle test rather than passing it quietly.
  `TestRevertingOneDatabaseLeavesTheClusterAlone` reverts one database of a shared cluster and
  checks that the neighbour keeps its version, its objects and its grants — and that the
  reverted database can be applied again, which a dirty `-1` would refuse.
