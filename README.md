# Wagering service

Processes wagering operations submitted by game providers against player
wallets, keeping every balance change audited and every submission idempotent.

An operation arrives over HTTP or on a FIFO queue, is applied inside one
database transaction against one wallet, and produces a wager transaction, a
ledger entry and the events an outbox publisher puts on a second queue. Money is
`int64` minor units at a fixed scale of two and crosses every wire as a
two-decimal string; there is no float on any money path anywhere in this tree.

- [`CONTEXT.md`](CONTEXT.md) — the ubiquitous language. The words below are all
  from it.
- [`ARCHITECTURE.md`](ARCHITECTURE.md) — the HTTP contract, the design and its
  reasoning, the interpretations, the limitations.
- [`docs/adr/`](docs/adr) — thirteen decisions, each with what was rejected and
  why.
- [`docs/schema.md`](docs/schema.md) — the schema, its roles, and what is
  deliberately not in it.

---

## Prerequisites

| | Version used here | Why |
|---|---|---|
| Go | 1.27 | `go.mod` names it. |
| Docker | 29.4.3 | The stack, and the containers the tests start. |
| Docker Compose | v5.1.4 | `docker compose`, not `docker-compose`. |
| `golangci-lint` | 2.13.2 | Only for `make lint`, `make check` and `make fix`. |

Nothing else is needed — no `jq`, no AWS CLI, no `migrate` binary. The
migrations are embedded in `cmd/migrate`, the queues are provisioned by
LocalStack's own init hook, and `make token` extracts a token with `sed`.

Docker is given 8 CPUs and 8 GB here. That is comfortable; the integration
suite starts ten containers and is where a smaller runner begins to matter.

---

## The whole stack, in one command

```
make up
```

which is `docker compose up --build --detach --wait` on the five services that
pull in the other seven, followed by `docker compose ps`. It waits for Grafana
as well as the API, because Grafana is by some distance the slowest thing here
to start and a target that returned before it was ready would hand you a
dashboard forty seconds from answering.

```
NAME                        SERVICE          STATUS                    PORTS
wagering-api-1-1            api-1            Up (healthy)              0.0.0.0:8081->8080/tcp
wagering-api-2-1            api-2            Up (healthy)              0.0.0.0:8082->8080/tcp
wagering-api-3-1            api-3            Up (healthy)              0.0.0.0:8083->8080/tcp
wagering-grafana-1          grafana          Up (healthy)              0.0.0.0:3000->3000/tcp
wagering-keycloak-1         keycloak         Up (healthy)              0.0.0.0:8080->8080/tcp
wagering-localstack-1       localstack       Up (healthy)              0.0.0.0:4566->4566/tcp
wagering-otel-collector-1   otel-collector   Up (healthy)              0.0.0.0:4317-4318->4317-4318/tcp
wagering-postgres-1         postgres         Up (healthy)              0.0.0.0:5432->5432/tcp
wagering-prometheus-1       prometheus       Up (healthy)              0.0.0.0:9090->9090/tcp
wagering-tempo-1            tempo            Up (healthy)              0.0.0.0:3200->3200/tcp
wagering-worker-1           worker           Up
wagering-worker-2           worker           Up
```

Three API replicas rather than one, because several of this service's guarantees
are about more than one process: the wallet row lock, the outbox claim, the
inbox. They are three service definitions sharing a YAML anchor rather than
`deploy.replicas: 3`, because replicas of one service all publish the same host
port — and a port *range* binds but assigns by start order, so a demonstration
that submits to one replica and reads back from another could not say which was
which. Two worker replicas, which can use `deploy.replicas`, because a worker
publishes nothing and has nothing to collide over.

The worker has no healthcheck: it serves nothing, and its start-up — the pool,
the queue resolution, the loops — is its check. A worker that is `Up` has
already proved everything a probe could ask it.

```
make down    # stop, keeping the database volume
make clean   # stop, and delete the volume with it
make logs    # follow everything
make help    # every target, with one line each
```

`docker compose up --build` on its own does the same thing without the waiting,
and is what to run when you want the logs in front of you.

---

## Environment variables

Every variable the service reads is written out in
[`.env.example`](.env.example) at its default, with a paragraph on each saying
what it is for. Copy it to `.env` and change what you need; a line you delete
changes nothing, because the file states defaults rather than overriding them.
(The one variable with no line of its own is `HOSTNAME`, which a container
runtime sets and which only `PUBLISHER_NAME` falls back to — its paragraph says
so.)

**Four are required and have no default:** `DATABASE_URL`, `AWS_REGION`,
`OIDC_ISSUER` and `OIDC_AUDIENCE`. A process that starts at all was therefore
configured deliberately rather than falling back to somewhere.

A variable that is **set and empty counts as unset**, because `FOO=${BAR}` in a
compose file with no `BAR` is an empty `FOO`, and a default that a substitution
could silently defeat is not a default. The loader collects every problem it
finds and reports them together, so a misconfigured deployment is fixed once
rather than one variable per restart.

Three of them are worth knowing before you read the rest:

| Variable | |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | **Empty means nothing is exported at all**, which is the service's own default. A default of "somewhere" makes a laptop, a unit test and a CI runner all retry a collector nobody started. `docker-compose.yml` sets it to `http://otel-collector:4317`; `.env.example` sets it to the same collector seen from the host. |
| `CONSUMER_NAME` | Must be **identical** across every replica. The inbox holds one row per consumer per message, and that row is what turns a redelivery into a replay; a name derived from `HOSTNAME` would give each replica its own rows and let one message be applied once by each of them. |
| `PUBLISHER_NAME` | Must be **distinct** per process, for the opposite reason: it lands in the outbox claim and scopes a reschedule to the publisher holding the row. It has no fixed default — it falls back to `HOSTNAME`, and with neither set the process refuses to start. A fixed default would collide silently the day a second replica deployed. |

`docker-compose.yml` writes its configuration inline rather than pointing
`env_file` at `.env.example`, and the two disagree on purpose: `.env.example` is
the *host's* view — `localhost:5432`, `localhost:8080` — because that is where a
developer running `go run ./cmd/api` finds these services, while in compose
every address is a service name on the compose network. Pointing `env_file` at
the example file would also inherit one `PUBLISHER_NAME` into both worker
replicas.

---

## Queue provisioning

Nothing to run. LocalStack executes
[`deploy/localstack/01-queues.sh`](deploy/localstack/01-queues.sh) once its edge
service is accepting requests, and that script creates all three queues:

| Queue | |
|---|---|
| `wager-transactions.fifo` | what the consumer reads. `MessageGroupId` is the **wallet id**; `MessageDeduplicationId` is the envelope's **messageId**. Visibility 30s, redrive to the DLQ on the fifth receipt, long polling at 20s. |
| `wager-transactions-dlq.fifo` | where a message goes once its five deliveries are spent. |
| `wallet-events.fifo` | where the outbox publisher sends. `MessageGroupId` is the **aggregate id**, which is always a wallet; `MessageDeduplicationId` is the **eventId**, which is stable across republication — so a publisher killed between sending and marking the outbox row cannot put a second copy on the wire when it comes back. |

`ContentBasedDeduplication` is **off** on all three. The envelope's own
`messageId` is the identity the inbox keys on, and a content hash would make two
bodies differing only in whitespace into two messages, which is the opposite of
what the inbox says they are.

The script's header comment is where those choices are argued at length. To see
what it made:

```
$ docker compose exec localstack awslocal sqs list-queues --output text --query 'QueueUrls[]'
http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions-dlq.fifo
http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions.fifo
http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wallet-events.fifo
```

`SQS_VISIBILITY_TIMEOUT` and `SQS_MAX_RECEIVE_COUNT` in the service's own
configuration restate two of that script's numbers, and nothing cross-checks
them against the provisioned queue at start-up. Keep them in step by hand.

---

## Migrations

`make up` runs them: `docker-compose.yml` has a `migrate` job that applies every
migration and exits, and every service that needs a schema depends on its
successful completion. You only run these by hand when you are working against a
database outside the stack.

```
make migrate-up        # apply every migration not yet applied
make migrate-down      # revert every applied migration
make migrate-version   # report the applied version
```

Each takes `DATABASE_URL`, defaulting to the stack's PostgreSQL on the published
port. Against a database created for the purpose:

```
$ SCRATCH='postgres://postgres:postgres@localhost:5432/wagering_migrate_demo?sslmode=disable'
$ make migrate-up      DATABASE_URL="$SCRATCH"
8
$ make migrate-version DATABASE_URL="$SCRATCH"
8
$ make migrate-down    DATABASE_URL="$SCRATCH"
0
$ make migrate-version DATABASE_URL="$SCRATCH"
0
```

**The migration connection is not the service's connection.** `DATABASE_URL`
here must connect as a member of `wagering_migrator`, which owns the schema; the
service connects as a member of `wagering_app`, which has no DDL and no `UPDATE`
or `DELETE` on the ledger. That is why `.env.example`'s `DATABASE_URL` carries
`options=-c%20role%3Dwagering_app` and the Makefile's does not — connecting the
service as the owner would quietly remove half of what makes the ledger
append-only. [`docs/schema.md`](docs/schema.md) has the grant table.

A full revert leaves both roles standing, holding nothing in this database. That
is ADR-0009 and not an oversight: roles are cluster-wide and a migration is
per-database, so one database's revert may not decide the cluster is finished
with them.

```
make db-up / make db-down   # a bare PostgreSQL 16 to migrate against
```

---

## Obtaining a token

Every route but the two health checks needs a bearer token from the Keycloak
realm in [`deploy/keycloak/realm-export.json`](deploy/keycloak/realm-export.json),
which is imported at start-up. All four clients are confidential service
accounts, so this is the `client_credentials` grant and there is no user in it.

```
make token CLIENT=provider-a
```

prints the access token and nothing else, so that it composes:

```
curl -H "Authorization: Bearer $(make token CLIENT=provider-b)" ...
```

| `CLIENT` | Realm role | `providerId` | What it can do |
|---|---|---|---|
| `provider-a` | `provider` | `provider-a` | Submit and read its own operations. |
| `provider-b` | `provider` | `provider-b` | The same, as a different provider — the one to use to show that a provider cannot read another's. |
| `wallet-service` | `internal` | — | Open, read, page and reconcile wallets, and read every provider's operations. It submits as no provider. |
| `provider-expiring` | `provider` | `provider-expiring` | Exists only so a test can present an expired token. |

Every secret is the client id with `-secret` after it — `provider-a-secret` and
so on. That is a documented placeholder and could not be anything else: the
realm is imported from a file in this repository. A production realm is built
from this file's *shape*, never from its values.

The role is read from `realm_access.roles` and from nowhere else. `azp` is
present in every one of these tokens and is deliberately never read: it names
the client that asked for the token, not the grant the token carries.

Tokens last five minutes. A 401 halfway through a session is usually that.

---

## Calling the API

The three replicas answer on **8081**, **8082** and **8083**, and everything
below works against any of them — which is the point of there being three.

Every response below — here, in *Submitting over the queue*, and in *Grafana* —
is from a real run of exactly these calls, against the stack `make up` brings
up. Two things are edited, and nothing else: the bodies are wrapped to fit the
page, and the run-scoped suffix is dropped from the identifiers a *caller* chose
— `player-demo` was `player-demo-1790032067`, and likewise for the external ids,
the idempotency keys and the message id. Everything the
**service** produced — every transaction id, timestamp, status, failure code and
balance — is as it came back.

```
INTERNAL=$(make token CLIENT=wallet-service)
PROVIDER=$(make token CLIENT=provider-a)
```

### `POST /wallets` — role `internal`

A wallet is a player's balance in **one currency**, so `initialBalance` names
the currency and is required. A wallet with nothing in it is opened at `"0.00"`.

```
$ curl -i -X POST http://localhost:8081/wallets \
    -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
    -H 'X-Correlation-Id: demo-open' \
    -d '{"playerId":"player-demo","initialBalance":{"amount":"100.00","currency":"BRL"}}'

HTTP/1.1 201 Created
Content-Type: application/json
Location: /wallets/01a0c639-bc89-774b-9b4c-7a1de7643a09
X-Correlation-Id: demo-open

{"walletId":"01a0c639-bc89-774b-9b4c-7a1de7643a09","playerId":"player-demo",
 "balance":{"amount":"100.00","currency":"BRL"},"version":1,
 "createdAt":"2026-09-21T23:07:47.722729Z","updatedAt":"2026-09-21T23:07:47.722729Z",
 "opening":{"transactionId":"01a0c639-bc89-7754-999a-2134c115c377","kind":"OPENING",
  "status":"PROCESSED","money":{"amount":"100.00","currency":"BRL"},
  "balance":{"amount":"100.00","currency":"BRL"},"idempotentReplay":false}}
```

`opening` is the internal wager transaction that records the starting balance.
It is absent for a wallet opened at `"0.00"`, because an opening records a
starting balance and a wallet opened at zero has none.

### A full BET → WIN → REFUND → ROLLBACK

Four submissions, one round, on the wallet above. Each one is legal *given the
ones before it*, and the sequence is chosen to show why: a refund reverses a
bet, a rollback undoes a transaction, and a reference may carry only one
**active reversal** at a time.

Every submission carries `Idempotency-Key`. It is read from the header and
nowhere else, is never trimmed, and is never computed from the body — deriving
one would make every distinct payload its own key, which is the opposite of what
the header is for.

**1. `BET` 25.00** — debits. `100.00 → 75.00`.

```
$ curl -i -X POST http://localhost:8081/wagering/transactions \
    -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
    -H 'Idempotency-Key: demo-key-bet' -H 'X-Correlation-Id: demo-flow' \
    -d '{"provider":"provider-a","externalTransactionId":"demo-bet","playerId":"player-demo",
         "roundId":"demo-round","gameId":"lucky-sevens","kind":"BET",
         "money":{"amount":"25.00","currency":"BRL"}}'

HTTP/1.1 200 OK
X-Correlation-Id: demo-flow

{"transactionId":"01a0c639-cccf-766c-8be6-85e750289a60","externalTransactionId":"demo-bet",
 "kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},
 "balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}
```

**2. `WIN` 40.00, naming the bet** — credits. `75.00 → 115.00`. A win may name
the bet it pays out on, in the same round. It is not a reversal: it need not
match the stake, several wins may point at one bet, and it does **not** hold the
bet — so the bet is still refundable afterwards.

```
$ curl -i -X POST http://localhost:8082/wagering/transactions \
    -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
    -H 'Idempotency-Key: demo-key-win' -H 'X-Correlation-Id: demo-flow' \
    -d '{"provider":"provider-a","externalTransactionId":"demo-win","playerId":"player-demo",
         "roundId":"demo-round","gameId":"lucky-sevens","kind":"WIN",
         "money":{"amount":"40.00","currency":"BRL"},
         "referenceExternalTransactionId":"demo-bet"}'

HTTP/1.1 200 OK

{"transactionId":"01a0c639-d523-77ec-b3c9-7b0ffa9a6f5e","externalTransactionId":"demo-win",
 "kind":"WIN","status":"PROCESSED","money":{"amount":"40.00","currency":"BRL"},
 "balance":{"amount":"115.00","currency":"BRL"},"idempotentReplay":false}
```

**3. `REFUND` 25.00 of the bet** — returns the stake. `115.00 → 140.00`. A
refund reverses a bet and nothing else, and it must return **exactly** what the
bet moved: partial reversals do not exist.

```
$ curl -i -X POST http://localhost:8083/wagering/transactions \
    -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
    -H 'Idempotency-Key: demo-key-refund' -H 'X-Correlation-Id: demo-flow' \
    -d '{"provider":"provider-a","externalTransactionId":"demo-refund","playerId":"player-demo",
         "roundId":"demo-round","gameId":"lucky-sevens","kind":"REFUND",
         "money":{"amount":"25.00","currency":"BRL"},
         "referenceExternalTransactionId":"demo-bet"}'

HTTP/1.1 200 OK

{"transactionId":"01a0c639-ddc3-7a55-bef6-196a89e2085f","externalTransactionId":"demo-refund",
 "kind":"REFUND","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},
 "balance":{"amount":"140.00","currency":"BRL"},"idempotentReplay":false}
```

**3a. A `ROLLBACK` of the *bet*, now — refused, and this is the interesting
one.** The refund is an active reversal of the bet, so rolling the bet back as
well would return the same 25.00 twice. It is refused as a **business outcome**
rather than as an error: a wager transaction is recorded,
`WagerTransactionRejected` is published, the idempotency key is bound to this
payload for good, and the answer is 422 with the failure code in the body.

```
$ curl -i -X POST http://localhost:8081/wagering/transactions \
    -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
    -H 'Idempotency-Key: demo-key-rollback-bet' -H 'X-Correlation-Id: demo-flow' \
    -d '{"provider":"provider-a","externalTransactionId":"demo-rollback-bet","playerId":"player-demo",
         "roundId":"demo-round","gameId":"lucky-sevens","kind":"ROLLBACK",
         "money":{"amount":"25.00","currency":"BRL"},
         "referenceExternalTransactionId":"demo-bet"}'

HTTP/1.1 422 Unprocessable Entity

{"transactionId":"01a0c639-eadc-7dd0-a955-051f510ab872",
 "externalTransactionId":"demo-rollback-bet","kind":"ROLLBACK","status":"REJECTED",
 "money":{"amount":"25.00","currency":"BRL"},"failureCode":"REFERENCE_ALREADY_REVERSED",
 "idempotentReplay":false}
```

**4. `ROLLBACK` 25.00 of the *refund*** — undoes the refund, debiting the 25.00
back out. `140.00 → 115.00`. This is the legal rollback, and undoing the refund
**releases the bet**: it becomes reversible again, which is the whole reason
"at most one reversal" is stated over *active* reversals rather than over
reversals. A rollback can never itself be reversed, so one applied straight to a
bet holds it permanently, while a refund can always be revisited.

```
$ curl -i -X POST http://localhost:8081/wagering/transactions \
    -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
    -H 'Idempotency-Key: demo-key-rollback' -H 'X-Correlation-Id: demo-flow' \
    -d '{"provider":"provider-a","externalTransactionId":"demo-rollback","playerId":"player-demo",
         "roundId":"demo-round","gameId":"lucky-sevens","kind":"ROLLBACK",
         "money":{"amount":"25.00","currency":"BRL"},
         "referenceExternalTransactionId":"demo-refund"}'

HTTP/1.1 200 OK

{"transactionId":"01a0c639-f58c-72eb-85c7-1562670dd246","externalTransactionId":"demo-rollback",
 "kind":"ROLLBACK","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},
 "balance":{"amount":"115.00","currency":"BRL"},"idempotentReplay":false}
```

`100.00 − 25.00 + 40.00 + 25.00 − 25.00 = 115.00`, and the wallet is at version
5 with five ledger entries.

### The four answers to a repeated submission

**Same key, same payload — a replay.** The stored outcome is answered again,
including the balance observed *then* rather than the wallet's balance now.
Nothing new is recorded.

```
$ curl -X POST http://localhost:8082/wagering/transactions \
    -H "Authorization: Bearer $PROVIDER" -H 'Idempotency-Key: demo-key-bet' \
    -H 'X-Correlation-Id: demo-flow' -d '{ …the bet, byte for byte… }'

{"transactionId":"01a0c639-cccf-766c-8be6-85e750289a60","externalTransactionId":"demo-bet",
 "kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},
 "balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":true}
```

**Same key, a different payload — 409.** The key is bound to the payload it was
first used for.

```
{"code":"IDEMPOTENCY_PAYLOAD_CONFLICT",
 "message":"idempotency key \"demo-key-bet\" is already bound to another operation",
 "correlationId":"01a0c63f-481b-7682-ad57-b738f315dfb3"}
```

**A different key, the same `externalTransactionId` — 409, with no failure
code.** The catalogue describes no outcome for it because nothing is persisted
for it, so a provider acts on the class rather than on a code invented for the
occasion.

```
{"code":"CONFLICT",
 "message":"operation \"demo-bet\" is already recorded under another idempotency key",
 "correlationId":"01a0c63f-4823-7b7c-b0f9-89c5e66d94aa"}
```

**A new key and a new operation** is the first case above, and is the only one
of the four that moves money.

### `GET /wagering/transactions/{transactionId}`

A provider sees its own; `internal` sees all. **A read is always 200**, whatever
the operation came to — the read succeeded, and `status` says what was read.

```
$ curl -i http://localhost:8081/wagering/transactions/01a0c639-cccf-766c-8be6-85e750289a60 \
    -H "Authorization: Bearer $PROVIDER"

HTTP/1.1 200 OK

{"transactionId":"01a0c639-cccf-766c-8be6-85e750289a60","externalTransactionId":"demo-bet",
 "kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},
 "balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}
```

### `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}`

The provider is in the path because an external id names an operation only
within the provider that issued it. A provider may read only as itself; the
service may read as any.

```
$ curl -i http://localhost:8081/providers/provider-a/wagering/transactions/demo-bet \
    -H "Authorization: Bearer $INTERNAL"

HTTP/1.1 200 OK

{"transactionId":"01a0c639-cccf-766c-8be6-85e750289a60","externalTransactionId":"demo-bet",
 "kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},
 "balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}
```

The same call with `provider-b`'s token is 403, answered from the token and the
path alone before anything is looked for — so it is the same answer for every
external id and confirms the existence of none of them.

### `GET /wallets/{walletId}` — role `internal`

```
$ curl -i http://localhost:8081/wallets/01a0c639-bc89-774b-9b4c-7a1de7643a09 \
    -H "Authorization: Bearer $INTERNAL"

HTTP/1.1 200 OK

{"walletId":"01a0c639-bc89-774b-9b4c-7a1de7643a09","playerId":"player-demo",
 "balance":{"amount":"115.00","currency":"BRL"},"version":5,
 "createdAt":"2026-09-21T23:07:47.722729Z","updatedAt":"2026-09-21T23:08:02.317963Z"}
```

### `GET /wallets/{walletId}/ledger?cursor=&limit=` — role `internal`

Oldest first, ordered by wallet version — which is an exact order, where
`createdAt` is not: two entries written in one transaction share a timestamp.
The cursor is opaque and is carried through untouched.

```
$ curl -i 'http://localhost:8081/wallets/01a0c639-bc89-774b-9b4c-7a1de7643a09/ledger?limit=3' \
    -H "Authorization: Bearer $INTERNAL"

HTTP/1.1 200 OK

{"walletId":"01a0c639-bc89-774b-9b4c-7a1de7643a09","entries":[
 {"ledgerEntryId":"01a0c639-bc89-7755-b87f-4bfd9c6c577c",
  "transactionId":"01a0c639-bc89-7754-999a-2134c115c377","direction":"CREDIT",
  "money":{"amount":"100.00","currency":"BRL"},"balanceBefore":{"amount":"0.00","currency":"BRL"},
  "balanceAfter":{"amount":"100.00","currency":"BRL"},"walletVersion":1,
  "createdAt":"2026-09-21T23:07:47.722729Z"},
 {"ledgerEntryId":"01a0c639-cccf-7679-895e-f18faca22e63",
  "transactionId":"01a0c639-cccf-766c-8be6-85e750289a60","direction":"DEBIT",
  "money":{"amount":"25.00","currency":"BRL"},"balanceBefore":{"amount":"100.00","currency":"BRL"},
  "balanceAfter":{"amount":"75.00","currency":"BRL"},"walletVersion":2,
  "createdAt":"2026-09-21T23:07:51.889253Z"},
 {"ledgerEntryId":"01a0c639-d523-77f4-bad6-3d99056a81b3",
  "transactionId":"01a0c639-d523-77ec-b3c9-7b0ffa9a6f5e","direction":"CREDIT",
  "money":{"amount":"40.00","currency":"BRL"},"balanceBefore":{"amount":"75.00","currency":"BRL"},
  "balanceAfter":{"amount":"115.00","currency":"BRL"},"walletVersion":3,
  "createdAt":"2026-09-21T23:07:54.022861Z"}],
 "nextCursor":"MDFhMGM2MzktYmM4OS03NzRiLTliNGMtN2ExZGU3NjQzYTA5OjM"}
```

Pass `nextCursor` back as `cursor` for the next page. Its **absence** on the
last page is how a caller knows to stop; `entries` is `[]` on an empty page and
never `null`.

### `POST /wallets/{walletId}/reconciliation` — role `internal`

Checks a wallet's ledger against the wallet and **reports what it finds; it
never corrects anything.** 200 whether or not the wallet balances, because the
check ran and the report is the answer.

```
$ curl -i -X POST http://localhost:8081/wallets/01a0c639-bc89-774b-9b4c-7a1de7643a09/reconciliation \
    -H "Authorization: Bearer $INTERNAL"

HTTP/1.1 200 OK

{"walletId":"01a0c639-bc89-774b-9b4c-7a1de7643a09","consistent":true,
 "stored":{"amount":"115.00","currency":"BRL"},
 "reconstructed":{"amount":"115.00","currency":"BRL"},
 "difference":{"amount":"0.00","currency":"BRL"}}
```

`difference` is stored less reconstructed and is the one signed amount on this
contract. A finding that the stored records are wrong in a way that is not a
mere imbalance answers 500 with the finding's own code — that is an **audit
failure**, addressed to an operator, and there is no payload for anybody to
repair.

### `GET /health/live` and `GET /health/ready` — public

```
$ curl -i http://localhost:8081/health/live
HTTP/1.1 200 OK
{"status":"alive"}

$ curl -i http://localhost:8081/health/ready
HTTP/1.1 200 OK
{"status":"ready","checks":{"postgres":"ok","sqs":"ok"}}
```

Liveness reports on the process and asks nothing of anything else: a liveness
probe that checked a dependency restarts a healthy process whenever that
dependency is down, and does it to every replica at once. Readiness runs both
checks at the same time under one budget, and runs *both* even when the first
fails, so the body names them all. A failing check answers 503 with
`Retry-After: 1`, and its error is logged rather than published — a readiness
endpoint is usually reachable by more of a network than the service is.

Every refusal on every route has the same three members —
`{"code","message","correlationId"}` — and `ARCHITECTURE.md` documents each
status with a captured body.

---

## Submitting over the queue

The same use case, reached by the other door. There is no token on a queue: the
queue *is* the authorisation boundary, and the principal is a provider principal
minted from `data.provider`.

The envelope's `type` has one accepted value, `WagerTransactionSubmitted`. The
kind lives in `data.kind` exactly as it does over HTTP, so the envelope type
names what the message *is* rather than which operation it carries, and one
payload is not described in two places. The idempotency key is a **member** here
where HTTP takes it in a header, because there is no header on a queue.

```
$ cat > /tmp/body.json <<'EOF'
{"messageId":"demo-msg-1","type":"WagerTransactionSubmitted",
 "occurredAt":"2026-09-21T23:08:22Z",
 "data":{"provider":"provider-a","externalTransactionId":"demo-queue-win",
         "idempotencyKey":"demo-key-queue-win","playerId":"player-demo",
         "roundId":"demo-round","gameId":"lucky-sevens","kind":"WIN",
         "money":{"amount":"10.00","currency":"BRL"},
         "referenceExternalTransactionId":"demo-bet"}}
EOF

$ docker compose exec -T localstack awslocal sqs send-message \
    --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions.fifo \
    --message-group-id 01a0c639-bc89-774b-9b4c-7a1de7643a09 \
    --message-deduplication-id demo-msg-1 \
    --message-body "$(cat /tmp/body.json)"

{
    "MD5OfMessageBody": "c63edf98076f8b28e881982746bab198",
    "MessageId": "768f8975-ce50-4d9c-b07f-dc5dcad716c1",
    "SequenceNumber": "15376227406398358470"
}
```

`--message-group-id` is the **wallet id** and `--message-deduplication-id` is
the envelope's own `messageId`. Both are required: the queues have
`ContentBasedDeduplication` off, and the adapter refuses a send that omits
either.

A second or so later, the worker says so — the consumer's own line, which
carries the queue's `receiveCount` and whether this was a replay:

```
$ docker compose logs worker --since 3m | grep demo-msg-1

worker-1  | {"time":"2026-09-21T23:08:22.816137674Z","level":"INFO",
 "msg":"the operation was applied","service":"wagering","consumer":"wager-consumer",
 "queueMessageId":"768f8975-ce50-4d9c-b07f-dc5dcad716c1","receiveCount":1,
 "correlationId":"demo-msg-1","messageId":"demo-msg-1",
 "transactionId":"01a0c63a-4595-7c38-92c1-da80d68ebb64","kind":"WIN","status":"PROCESSED",
 "failureCode":"","replay":false,"traceId":"0f19f45dec87e96e3480bd88c3afa35e",
 "spanId":"1ae5ba2f74e5f592"}
```

and the operation reads back over HTTP exactly as an HTTP-submitted one does:

```
$ curl http://localhost:8081/providers/provider-a/wagering/transactions/demo-queue-win \
    -H "Authorization: Bearer $INTERNAL"

{"transactionId":"01a0c63a-4595-7c38-92c1-da80d68ebb64",
 "externalTransactionId":"demo-queue-win","kind":"WIN","status":"PROCESSED",
 "money":{"amount":"10.00","currency":"BRL"},"balance":{"amount":"125.00","currency":"BRL"},
 "idempotentReplay":false}
```

`115.00 + 10.00 = 125.00`. One operation, one wallet, whichever door it came in
by — which `TestOneOperationOverHTTPAndOverTheQueueSettlesOnceInEitherOrder`
proves for the same operation arriving over **both**, in either order.

---

## Grafana, and the trace for that flow

```
$ make dashboards
Grafana:  http://localhost:3000
user:     admin
password: admin
…
```

— and then goes on, in its own output, to say that reading needs no sign-in and
how to find a trace. Those three are `GRAFANA_URL`, `GRAFANA_USER` and
`GRAFANA_PASSWORD`; the fourth of these variables, `TEMPO_URL`
(<http://localhost:3200>), is the one `make trace` asks. All four are read by
the **Makefile**, not by the service, and are documented at the foot of
`.env.example` beside the ones that are — so a password a target prints is
written down somewhere other than that target. The credentials are
`docker-compose.yml`'s `GF_SECURITY_ADMIN_USER` and `GF_SECURITY_ADMIN_PASSWORD`
— placeholders, on a stack reachable from nowhere but the laptop running it.

Prometheus is at <http://localhost:9090> and no target prints it, because
nothing has to be told where it is to read the dashboard: Grafana reaches it
over the compose network.

**Reading the dashboard needs no sign-in.** Anonymous access is on; the
credentials are what anything that *writes* has to present. Both datasources and
the dashboard **Wagering service** are provisioned from `deploy/grafana`, so
there is nothing to import:

```
$ curl -s 'http://localhost:3000/api/search?query=Wagering'
[{"uid":"wagering","title":"Wagering service","url":"/d/wagering/wagering-service", …}]
```

`ARCHITECTURE.md` → *Observability* lists every panel with the query behind it,
and explains the two label renames (`exported_job`, `exported_instance`) that a
query here has to know about.

### Finding the trace for the flow above

Every span carries `correlationId` — the value `X-Correlation-Id` echoes, the
error body returns, and every log line names. Every submission above went in
under `demo-flow` — the four that moved money, the one that was refused, and the
replay — so:

```
$ make trace CORRELATION=demo-flow

{"traces":[
 {"traceID":"d27307a5c2bc512f813832e59f6a915a","rootServiceName":"wagering",
  "rootTraceName":"POST /wagering/transactions","durationMs":2, …},
 {"traceID":"34533739c8469d7a3d5310242c55dd6a", … "durationMs":479, …},
 …
]}
```

six traces, one per submission, each between 7 and 18 spans — the HTTP request,
the use case, the SQL transaction, and the wallet events the outbox publisher
put on the queue **seconds later, in another container**. They are in the same
trace because that is where the event came from.

In a browser it is *Explore* → the **Tempo** datasource → the **TraceQL** tab.
The query, the same query over `.transactionId` / `.walletId` / `.providerId` /
`.messageId` / `.eventId`, the shape of the span tree, and why `start` and `end`
are not optional on the search API are all in `ARCHITECTURE.md` → *Finding a
trace by `correlationId`*.

An operation submitted over the **queue** carries the envelope's `messageId` as
its correlation — `make trace CORRELATION=demo-msg-1` finds the one above.

---

## Tests

```
go vet ./...                             # under a second
go test ./...                            # ~10s   — needs Docker
go test -race ./...                      # ~11s   — needs Docker
go test -race -tags integration ./...    # ~57s   — ten containers
go test -race -tags multi ./...          # ~51s   — starts the compose stack itself
```

Wall clock, measured on this tree on 8 CPUs with the images pulled, the compose
stack already healthy and the build cache warm. `make test`, `make test-race`,
`make test-integration` and `make test-multi` are the same commands with
`-count=1` where it matters.

**Run the two container suites one at a time, not chained.** `integration` and
`multi` each pass repeatedly on their own, but `go test -tags integration ./...`
immediately followed by `go test -tags multi ./...` fails intermittently while
the daemon is still busy, with `dependency localstack failed to start` and a
`No such container` from Compose. Nothing is wrong with either suite. The
`integration` suites are testcontainers', which hands cleanup to a reaper that
force-removes containers by label *after* the test process has already exited,
so `go test` returns while the daemon is still deleting; the `multi` suite is
Compose's, and expects to own its project's containers and network from the
first command. The second `up` lands in the middle of the first suite's
teardown. Leave a few seconds between them, or run them in separate steps —
which is what `make test-integration` and `make test-multi` are for. CI runs
the untagged and `integration` suites as two separate steps and so never meets
this; it does not run the `multi` suite at all, because that suite brings up
Compose and the workflow has no Docker Compose stage. Running it is a local
step today.

**`go test ./...` already needs Docker.** `internal/storage/postgres` runs its
schema-conformance suite against a real PostgreSQL 16 through testcontainers,
with no build tag, because there is nothing to conform to without one.
`TEST_DATABASE_URL` points it at an existing cluster instead.

| Tag | What it adds | What it needs |
|---|---|---|
| *(none)* | The domain, the application layer, every adapter's unit tests, the composition root's wiring, and the schema-conformance suite. | Docker (one PostgreSQL). |
| `integration` | `internal/adapters/postgres` against the real schema, `internal/adapters/sqs` against LocalStack, `internal/messaging` for the queue path, `internal/integration` for the authenticated HTTP path and `internal/fxmod` for the whole graph — the last two against a real Keycloak running **this repository's own realm**. | Docker. Ten containers: five PostgreSQL, three LocalStack, two Keycloak — plus testcontainers' own reaper. |
| `multi` | `internal/multi` — three API instances and two workers, with independent connections and independent memory, against one database and one set of queues. Ten scenarios: four drive the compose replicas themselves, and six build a world of their own — a database, three FIFO queues, and processes this suite starts, arms with `FAULT_POINT`, kills and replaces. | Docker, and it brings the compose stack up itself. |

No mock, fake or in-memory substitute stands in for PostgreSQL, SQS or Keycloak
in any of them. Ordering, deadlock-freedom, `SKIP LOCKED` and the deferred
COMMIT-time triggers are container tests or nothing, and the unit tests say so
rather than pretending to cover them.

The `multi` suite needs nothing started first: it runs `docker compose up
--build --detach --wait` itself, which is idempotent and costs about two seconds
against a stack that is already healthy, then builds `cmd/api` and `cmd/worker`
with the race detector — because the binaries it drives are the thing under
test. The package itself takes **43 to 46 seconds** warm, of which the ten
scenarios are about thirty; the `./...` figure above is that plus every other
package's tests. Budget about ninety seconds when Compose has to rebuild an
image, which it does on any run where a file the `Dockerfile` copies has
changed, and three minutes from nothing at all.
`internal/multi/doc.go` breaks that down.

```
make check    # gofmt -l . && go vet ./... && golangci-lint run && go test -race ./...
make cover    # coverage per function
make fuzz     # fuzz the two parsers that read provider input, FUZZTIME each
make vuln     # govulncheck
```

`make check` uses `test-race` rather than `test`, because the race detector is
the part of this gate that finds what review does not.

---

## Layout

| | |
|---|---|
| `internal/domain/{money,failure,wagering}` | The model. No I/O, no framework, no dependency outside the standard library. |
| `internal/app` | The two application services, the ports every adapter implements, the error class model. It decides nothing about money and everything about when a decision is persisted. |
| `internal/adapters/{postgres,sqs,http,oidc}` | One port implementation each. Each `doc.go` states its own contract. |
| `internal/workers` | The three loops: the consumer, the outbox publisher, the reference worker. |
| `internal/fxmod` | The composition root. It is where the graph is assembled, and nothing below it imports a dependency-injection framework — which is what lets every other package be built by hand in a test. |
| `internal/telemetry` | The logger and the instruments every component reports through. |
| `internal/{integration,messaging,multi}` | The suites that cross packages, each in a package of its own so that a failure's first question — *whose fault?* — is not answered wrongly by where the file lives. |
| `cmd/{api,worker,migrate}` | Three binaries. |
| `migrations/` | Eight migrations, embedded in `cmd/migrate`. |
| `deploy/` | Keycloak's realm, LocalStack's queues, and the observability stack's configuration. |
