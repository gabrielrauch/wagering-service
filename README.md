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
publishes no port and has nothing to collide over.

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
| `wager-transactions.fifo` | what the consumer reads. `MessageGroupId` is the **wallet id**; `MessageDeduplicationId` is the envelope's **messageId**. Visibility 30s, long polling at 20s, and a redrive policy that moves a message to the DLQ when its receive count **exceeds** five — the fifth delivery is still handled, and it is when that one is spent that the message moves. |
| `wager-transactions-dlq.fifo` | where a message goes once its five deliveries are spent. |
| `wallet-events.fifo` | where the outbox publisher sends. `MessageGroupId` is the **aggregate id**, which is always a wallet; `MessageDeduplicationId` is the **eventId**, which is stable across republication — so a publisher killed between sending and marking the outbox row cannot put a second copy on the wire when it comes back. |

`ContentBasedDeduplication` is **off** on all three. The envelope's own
`messageId` is the identity the inbox keys on, and a content hash would make two
bodies differing only in whitespace into two messages, which is the opposite of
what the inbox says they are.

The script also attaches a **resource policy** to each queue, for three IAM
roles: `wagering-producer` may `SendMessage` on the inbound queue and nothing
else; `wagering-worker` may receive, delete and change the visibility of
inbound messages, send to `wallet-events.fifo`, and is the only role that may
receive from the DLQ; `wagering-api` may only `GetQueueUrl` and
`GetQueueAttributes` on the two live queues, for the start-up check and the
readiness probe, and may send nowhere. No statement grants to `*` and there is
no `Deny`. `docker-compose.yml` sets each service's `AWS_ACCESS_KEY_ID` to the
name of its role so the intended identity is visible where the credentials
are. LocalStack Community provisions the policy and reads it back — a test
checks every grant — but does not enforce it, so a denied call cannot be shown
locally; on AWS the three roles have to exist before the script runs.

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
10
$ make migrate-version DATABASE_URL="$SCRATCH"
10
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
append-only. The service checks this for itself at start-up: a process whose
connection can `UPDATE` or `DELETE` `wagering.wallet_ledger_entry` refuses to
start, saying *"the database connection can rewrite the ledger; connect as a
member of wagering_app — DATABASE_URL should carry options=-c
role=wagering_app"*. [`docs/schema.md`](docs/schema.md) has the grant table.

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
— `player-demo` was `player-demo-1790056534`, and likewise for the external ids,
the idempotency keys and the message id. Everything the
**service** produced — every transaction id, wallet id, timestamp, status,
failure code and balance — is as it came back.

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
Location: /wallets/01a0c7af-13bb-7af5-9257-0ca1bad355bb
X-Correlation-Id: demo-open

{"id":"01a0c7af-13bb-7af5-9257-0ca1bad355bb","playerId":"player-demo",
 "balance":{"amount":"100.00","currency":"BRL"},"version":1,
 "createdAt":"2026-09-22T05:55:34.973706Z","updatedAt":"2026-09-22T05:55:34.973706Z",
 "opening":{"transactionId":"01a0c7af-13bb-7b00-a60e-969b2c208fc9","kind":"OPENING",
  "status":"PROCESSED","money":{"amount":"100.00","currency":"BRL"},
  "balance":{"amount":"100.00","currency":"BRL"},"idempotentReplay":false}}
```

`id` is the wallet's identifier — the one every submission below names as its
`walletId`. `opening` is the internal wager transaction that records the
starting balance. It is absent for a wallet opened at `"0.00"`, because an
opening records a starting balance and a wallet opened at zero has none.

The currency has to be one a fixed scale of two can hold — one of the ISO 4217
codes with two minor-unit digits. `JPY` is refused with 400
`UNSUPPORTED_CURRENCY`, and so is a well-formed code that names nothing.

### A full BET → WIN → REFUND → ROLLBACK

Four submissions, one round, on the wallet above. Each one is legal *given the
ones before it*, and the sequence is chosen to show why: a refund reverses a
bet, a rollback undoes a transaction, a reference may carry only one **active
reversal** at a time, and it receives at most one **successful reversal of each
kind**.

Every submission carries `Idempotency-Key`. It is read from the header and
nowhere else, is never trimmed, and is never computed from the body — deriving
one would make every distinct payload its own key, which is the opposite of what
the header is for. Every submission also names its `walletId`: the operation is
applied to that wallet and to no other, and a wallet that does not exist or
that the player does not hold is one 404, with nothing recorded.

**1. `BET` 25.00** — debits. `100.00 → 75.00`.

```
$ curl -i -X POST http://localhost:8081/wagering/transactions \
    -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
    -H 'Idempotency-Key: demo-key-bet' -H 'X-Correlation-Id: demo-flow' \
    -d '{"providerId":"provider-a","externalTransactionId":"demo-bet","playerId":"player-demo",
         "walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb",
         "roundId":"demo-round","gameId":"lucky-sevens","kind":"BET",
         "money":{"amount":"25.00","currency":"BRL"}}'

HTTP/1.1 200 OK
X-Correlation-Id: demo-flow

{"transactionId":"01a0c7af-13d4-71e0-b657-f8f38cd10250","externalTransactionId":"demo-bet",
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
    -d '{"providerId":"provider-a","externalTransactionId":"demo-win","playerId":"player-demo",
         "walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb",
         "roundId":"demo-round","gameId":"lucky-sevens","kind":"WIN",
         "money":{"amount":"40.00","currency":"BRL"},
         "referenceExternalTransactionId":"demo-bet"}'

HTTP/1.1 200 OK

{"transactionId":"01a0c7af-13e4-7fa3-bae8-01f1b6220e05","externalTransactionId":"demo-win",
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
    -d '{"providerId":"provider-a","externalTransactionId":"demo-refund","playerId":"player-demo",
         "walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb",
         "roundId":"demo-round","gameId":"lucky-sevens","kind":"REFUND",
         "money":{"amount":"25.00","currency":"BRL"},
         "referenceExternalTransactionId":"demo-bet"}'

HTTP/1.1 200 OK

{"transactionId":"01a0c7af-13f8-71c1-9155-82c88799b2e5","externalTransactionId":"demo-refund",
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
    -d '{"providerId":"provider-a","externalTransactionId":"demo-rollback-bet","playerId":"player-demo",
         "walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb",
         "roundId":"demo-round","gameId":"lucky-sevens","kind":"ROLLBACK",
         "money":{"amount":"25.00","currency":"BRL"},
         "referenceExternalTransactionId":"demo-bet"}'

HTTP/1.1 422 Unprocessable Entity

{"transactionId":"01a0c7af-140a-7f7a-b7d5-53837bfd827c",
 "externalTransactionId":"demo-rollback-bet","kind":"ROLLBACK","status":"REJECTED",
 "money":{"amount":"25.00","currency":"BRL"},"failureCode":"REFERENCE_ALREADY_REVERSED",
 "idempotentReplay":false}
```

**4. `ROLLBACK` 25.00 of the *refund*** — undoes the refund, debiting the 25.00
back out. `140.00 → 115.00`. This is the legal rollback, and undoing the refund
**releases the bet** — to a rollback, and to nothing else. It is reversible
again, which is why "at most one reversal" is stated over *active* reversals
rather than over reversals; but a second `REFUND` of it would be refused with
the same `REFERENCE_ALREADY_REVERSED`, because a reference never receives two
successful reversals of one kind, undone or not. A rollback can never itself be
reversed, so one applied straight to a bet holds it permanently, while a refund
can be revisited once.

```
$ curl -i -X POST http://localhost:8081/wagering/transactions \
    -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
    -H 'Idempotency-Key: demo-key-rollback' -H 'X-Correlation-Id: demo-flow' \
    -d '{"providerId":"provider-a","externalTransactionId":"demo-rollback","playerId":"player-demo",
         "walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb",
         "roundId":"demo-round","gameId":"lucky-sevens","kind":"ROLLBACK",
         "money":{"amount":"25.00","currency":"BRL"},
         "referenceExternalTransactionId":"demo-refund"}'

HTTP/1.1 200 OK

{"transactionId":"01a0c7af-1416-7d17-96f0-f51e1ce649f0","externalTransactionId":"demo-rollback",
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

{"transactionId":"01a0c7af-13d4-71e0-b657-f8f38cd10250","externalTransactionId":"demo-bet",
 "kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},
 "balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":true}
```

**Same key, a different payload — 409.** The key is bound to the payload it was
first used for. (The bet again, at `26.00`, under `demo-flow`.)

```
{"code":"IDEMPOTENCY_PAYLOAD_CONFLICT",
 "message":"idempotency key \"demo-key-bet\" is already bound to another operation",
 "correlationId":"demo-flow"}
```

**A different key, the same `externalTransactionId` — 409, with no failure
code.** The catalogue describes no outcome for it because nothing is persisted
for it, so a provider acts on the class rather than on a code invented for the
occasion.

```
{"code":"CONFLICT",
 "message":"operation \"demo-bet\" is already recorded under another idempotency key",
 "correlationId":"demo-flow"}
```

**A new key and a new operation** is the first case above, and is the only one
of the four that moves money.

### `GET /wagering/transactions/{transactionId}`

A provider sees its own; `internal` sees all. **A read is always 200**, whatever
the operation came to — the read succeeded, and `status` says what was read.

```
$ curl -i http://localhost:8081/wagering/transactions/01a0c7af-13d4-71e0-b657-f8f38cd10250 \
    -H "Authorization: Bearer $PROVIDER"

HTTP/1.1 200 OK

{"transactionId":"01a0c7af-13d4-71e0-b657-f8f38cd10250","externalTransactionId":"demo-bet",
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

{"transactionId":"01a0c7af-13d4-71e0-b657-f8f38cd10250","externalTransactionId":"demo-bet",
 "kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},
 "balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}
```

The same call with `provider-b`'s token is 403, answered from the token and the
path alone before anything is looked for — so it is the same answer for every
external id and confirms the existence of none of them.

### `GET /wallets/{walletId}` — role `internal`

```
$ curl -i http://localhost:8081/wallets/01a0c7af-13bb-7af5-9257-0ca1bad355bb \
    -H "Authorization: Bearer $INTERNAL"

HTTP/1.1 200 OK

{"id":"01a0c7af-13bb-7af5-9257-0ca1bad355bb","playerId":"player-demo",
 "balance":{"amount":"115.00","currency":"BRL"},"version":5,
 "createdAt":"2026-09-22T05:55:34.973706Z","updatedAt":"2026-09-22T05:55:35.063265Z"}
```

### `GET /wallets/{walletId}/ledger?cursor=&limit=` — role `internal`

Oldest first, ordered by wallet version — which is an exact order, where
`createdAt` is not: two entries written in one transaction share a timestamp.
The cursor is opaque and is carried through untouched.

```
$ curl -i 'http://localhost:8081/wallets/01a0c7af-13bb-7af5-9257-0ca1bad355bb/ledger?limit=3' \
    -H "Authorization: Bearer $INTERNAL"

HTTP/1.1 200 OK

{"walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb","entries":[
 {"id":"01a0c7af-13bb-7b01-a3fb-f9904d569f15","walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb",
  "transactionId":"01a0c7af-13bb-7b00-a60e-969b2c208fc9","direction":"CREDIT",
  "money":{"amount":"100.00","currency":"BRL"},"balanceBefore":{"amount":"0.00","currency":"BRL"},
  "balanceAfter":{"amount":"100.00","currency":"BRL"},"walletVersion":1,
  "createdAt":"2026-09-22T05:55:34.973706Z"},
 {"id":"01a0c7af-13d4-71e9-afc1-ab6353b16345","walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb",
  "transactionId":"01a0c7af-13d4-71e0-b657-f8f38cd10250","direction":"DEBIT",
  "money":{"amount":"25.00","currency":"BRL"},"balanceBefore":{"amount":"100.00","currency":"BRL"},
  "balanceAfter":{"amount":"75.00","currency":"BRL"},"walletVersion":2,
  "createdAt":"2026-09-22T05:55:34.997062Z"},
 {"id":"01a0c7af-13e4-7faf-b5e8-e58262fceca8","walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb",
  "transactionId":"01a0c7af-13e4-7fa3-bae8-01f1b6220e05","direction":"CREDIT",
  "money":{"amount":"40.00","currency":"BRL"},"balanceBefore":{"amount":"75.00","currency":"BRL"},
  "balanceAfter":{"amount":"115.00","currency":"BRL"},"walletVersion":3,
  "createdAt":"2026-09-22T05:55:35.015469Z"}],
 "nextCursor":"MDFhMGM3YWYtMTNiYi03YWY1LTkyNTctMGNhMWJhZDM1NWJiOjM"}
```

Pass `nextCursor` back as `cursor` for the next page. Its **absence** on the
last page is how a caller knows to stop; `entries` is `[]` on an empty page and
never `null`.

### `POST /wallets/{walletId}/reconciliation` — role `internal`

Checks a wallet's ledger against the wallet and **reports what it finds; it
never corrects anything.** 200 whether or not the wallet balances, because the
check ran and the report is the answer.

```
$ curl -i -X POST http://localhost:8081/wallets/01a0c7af-13bb-7af5-9257-0ca1bad355bb/reconciliation \
    -H "Authorization: Bearer $INTERNAL"

HTTP/1.1 200 OK

{"walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb",
 "storedBalance":{"amount":"115.00","currency":"BRL"},
 "calculatedBalance":{"amount":"115.00","currency":"BRL"},
 "difference":{"amount":"0.00","currency":"BRL"},
 "consistent":true,"checkedEntries":5}
```

`storedBalance` is what the wallet row holds, `calculatedBalance` is its ledger
summed, `difference` is stored less calculated and is the one signed amount on
this contract, and `checkedEntries` is how many ledger entries the sum took in
— the opening included, which is why five entries make the 115.00 above. A
finding that the stored records are wrong in a way that is not a mere
imbalance answers 500 with the finding's own code — that is an **audit
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
queue *is* the authorisation boundary — the resource policy above says who may
send — and the principal is a provider principal minted from `data.providerId`.

The envelope's `type` has one accepted value, the specification's
`WagerTransactionRequested`. The kind lives in `data.kind` exactly as it does
over HTTP, so the envelope type names what the message *is* rather than which
operation it carries, and one payload is not described in two places. The
idempotency key is a **member** here where HTTP takes it in a header, because
there is no header on a queue; `data.walletId` is required, exactly as over
HTTP; and `occurredAt` is accepted with or without fractional seconds.

```
$ cat > /tmp/body.json <<'EOF'
{"messageId":"demo-msg-1","type":"WagerTransactionRequested",
 "occurredAt":"2026-09-22T05:55:35.000Z",
 "data":{"providerId":"provider-a","externalTransactionId":"demo-queue-win",
         "idempotencyKey":"demo-key-queue-win","playerId":"player-demo",
         "walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb",
         "roundId":"demo-round","gameId":"lucky-sevens","kind":"WIN",
         "money":{"amount":"10.00","currency":"BRL"},
         "referenceExternalTransactionId":"demo-bet"}}
EOF

$ docker compose exec -T localstack awslocal sqs send-message \
    --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions.fifo \
    --message-group-id 01a0c7af-13bb-7af5-9257-0ca1bad355bb \
    --message-deduplication-id demo-msg-1 \
    --message-body "$(cat /tmp/body.json)"

{
    "MD5OfMessageBody": "b9127a91a5af561fca9349382416c5f1",
    "MessageId": "3b61622e-4b5e-43ed-b643-bb3558636600",
    "SequenceNumber": "15376468551632158722"
}
```

`--message-group-id` is the **wallet id** — the same value as `data.walletId` —
and `--message-deduplication-id` is the envelope's own `messageId`. Both are
required: the queues have `ContentBasedDeduplication` off, and the adapter
refuses a send that omits either.

A second or so later, the worker says so — the consumer's own line, which
carries the queue's `receiveCount`, the operation's identity, and whether this
was a replay:

```
$ docker compose logs worker --since 3m | grep demo-msg-1

worker-1  | {"time":"2026-09-22T05:55:35.598869094Z","level":"INFO",
 "msg":"the operation was applied","service":"wagering","consumer":"wager-consumer",
 "queueMessageId":"3b61622e-4b5e-43ed-b643-bb3558636600","receiveCount":1,
 "correlationId":"demo-msg-1","messageId":"demo-msg-1",
 "transactionId":"01a0c7af-1625-7eda-bffc-4d8aca8cc8d1",
 "walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb","providerId":"provider-a",
 "kind":"WIN","status":"PROCESSED","failureCode":"","replay":false,
 "traceId":"98fd2ae42fe92a8ae57d97cfc658d609","spanId":"ad5b7872038948b6"}
```

The API writes the counterpart line for a submission it answered over HTTP —
the same identifiers, `"msg":"the operation was answered"` and
`"source":"http"` — so an operation is found in the logs by correlation,
transaction, wallet or provider whichever door it came in by. The bet at the top
of the flow left this one on `api-1`:

```
api-1-1  | {"time":"2026-09-22T05:55:34.999967219Z","level":"INFO",
 "msg":"the operation was answered","service":"wagering","source":"http",
 "correlationId":"demo-flow","transactionId":"01a0c7af-13d4-71e0-b657-f8f38cd10250",
 "walletId":"01a0c7af-13bb-7af5-9257-0ca1bad355bb","providerId":"provider-a",
 "kind":"BET","status":"PROCESSED","failureCode":"","replay":false,
 "traceId":"4b150f84225975bac162c1d578a0d6b9","spanId":"aaa8dc450e1a4e7e"}
```

Neither line carries an amount, a balance or a player: those are a financial
payload, and a log is not where one belongs.

The operation reads back over HTTP exactly as an HTTP-submitted one does:

```
$ curl http://localhost:8081/providers/provider-a/wagering/transactions/demo-queue-win \
    -H "Authorization: Bearer $INTERNAL"

{"transactionId":"01a0c7af-1625-7eda-bffc-4d8aca8cc8d1",
 "externalTransactionId":"demo-queue-win","kind":"WIN","status":"PROCESSED",
 "money":{"amount":"10.00","currency":"BRL"},"balance":{"amount":"125.00","currency":"BRL"},
 "idempotentReplay":false}
```

`115.00 + 10.00 = 125.00`. One operation, one wallet, whichever door it came in
by — which `TestOneOperationOverHTTPAndOverTheQueueSettlesOnceInEitherOrder`
proves for the same operation arriving over **both**, in either order, and
`TestOneOperationSubmittedOverHTTPAndTheQueueAtTheSameTimeSettlesOnce` for both
arriving at the same moment.

A message the consumer cannot read — not a JSON envelope, a `type` it does not
handle, a member the envelope does not have, a `messageId` already handled with
a different body — is never applied and never touched: one ERROR line with the
queue's message id, the receive count and the failure class, and the redrive
policy moves it to the DLQ once its five deliveries are spent.

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
under `demo-flow` — the four that moved money, the one that was refused, the
replay, and the two conflicts — so:

```
$ make trace CORRELATION=demo-flow

{"traces":[
 {"traceID":"8d1f93108776c182201975bba333b71e","rootServiceName":"wagering",
  "rootTraceName":"POST /wagering/transactions","durationMs":1, …},
 {"traceID":"849d3b91907fb3c682640779c1719f2", …},
 …
 {"traceID":"a876f1c85e5612b5c48418ec6ac8b17b", … "durationMs":2980, …},
 …
]}
```

eight traces, one per request, each between 7 and 18 spans — the HTTP request,
the use case, the SQL transaction, and for the ones that moved money the wallet
events the outbox publisher put on the queue **seconds later, in another
container**. They are in the same trace because that is where the event came
from, which is also why `durationMs` runs to seconds on those: it spans from
the request to the publisher's turn, not the request alone.

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
go test ./...                            # about 10s     — one PostgreSQL, or skips without one
go test -race ./...                      # about 11s     — the same
go test -race -tags integration ./...    # about 60s     — ten containers
go test -race -tags multi ./...          # 65 to 85s     — starts the compose stack itself
```

Approximate wall clock, measured on this tree on 8 CPUs with the images pulled,
the compose stack already healthy and the build cache warm; `Makefile` and
`internal/multi/doc.go` carry the same numbers. `make test`, `make test-race`,
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
which is what `make test-integration` and `make test-multi` are for. CI never
meets this: the untagged and `integration` suites are two steps of one job,
and `make test-multi` runs in a job of its own on a runner with Docker Compose,
which takes the stack down afterwards whatever happened.

**`go test ./...` wants Docker, and skips without it.** `internal/storage/postgres`
runs its schema-conformance suite against a real PostgreSQL 16 through
testcontainers, with no build tag, because there is nothing to conform to
without one. When no cluster can be started the suite **skips** and the run is
green with the schema never checked — Docker is a prerequisite for coverage,
not for passing. `TEST_DATABASE_URL` points it at an existing cluster instead,
which is what CI does, so that the skip cannot happen there.

| Tag | What it adds | What it needs |
|---|---|---|
| *(none)* | The domain, the application layer, every adapter's unit tests, the composition root's wiring, and the schema-conformance suite. | Docker (one PostgreSQL), or the suite skips. |
| `integration` | `internal/adapters/postgres` against the real schema, `internal/adapters/sqs` against LocalStack, `internal/messaging` for the queue path, `internal/integration` for the authenticated HTTP path and `internal/fxmod` for the whole graph — the last two against a real Keycloak running **this repository's own realm**. | Docker. Ten containers: five PostgreSQL, three LocalStack, two Keycloak — plus testcontainers' own reaper. |
| `multi` | `internal/multi` — three API instances and two workers, with independent connections and independent memory, against one database and one set of queues. Fourteen scenarios: seven drive the compose replicas themselves — two of them pause PostgreSQL and then LocalStack with `docker compose pause` and watch the deployment answer without them and recover — and seven build a world of their own — a database, three FIFO queues, and processes this suite starts, arms with `FAULT_POINT`, kills and replaces. | Docker, and it brings the compose stack up itself. |

No mock, fake or in-memory substitute stands in for PostgreSQL, SQS or Keycloak
in the container suites. Ordering, deadlock-freedom, `SKIP LOCKED` and the
deferred COMMIT-time triggers are container tests or nothing, and the untagged
unit suites — which fake the application layer's *ports*, since that is what
the ports are for — say so rather than pretending to cover them.

The `multi` suite needs nothing started first: it runs `docker compose up
--build --detach --wait` itself, which is idempotent and costs about two seconds
against a stack that is already healthy, then builds `cmd/api` and `cmd/worker`
with the race detector — because the binaries it drives are the thing under
test. Budget sixty-five to eighty-five seconds for the `./...` run warm, with
the fourteen scenarios about a minute of that; the spread is one delivery to
the deployment's own queue that LocalStack occasionally swallows and returns
only at the thirty-second visibility timeout. Budget about ninety seconds when
Compose has to rebuild an image, which it does on any run where a file the
`Dockerfile` copies has changed, and three minutes from nothing at all.
`internal/multi/doc.go` breaks that down.

```
make check    # gofmt -l . && go vet ./... && golangci-lint run && go test -race ./...
make cover    # coverage per function
make fuzz     # fuzz the two parsers that read provider input, FUZZTIME each
make vuln     # govulncheck
```

`make check` uses `test-race` rather than `test`, because the race detector is
the part of this gate that finds what review does not.

### Simulating a failure by hand

The recovery scenarios kill a real process at a named instant, and the same
instants are available to a worker you start yourself: `FAULT_POINT=<point> go
run ./cmd/worker` writes one line to standard error and exits with status 99
the moment it reaches that point. Five points exist — `after_commit_before_ack`,
`after_claim_before_publish`, `after_publish_before_mark`,
`after_pending_commit` and `before_commit` — and the `FAULT_POINT` block in
[`.env.example`](.env.example) says what each one proves and what to watch for
afterwards. `before_commit` fires on every movement transaction, including one
that wrote nothing, so a worker armed with it runs the consumer alone:
`FAULT_POINT=before_commit REFERENCE_WORKER_ENABLED=false
PUBLISHER_ENABLED=false go run ./cmd/worker`. The points are on the production
path — not behind a flag, not behind a build tag — and inert unless the variable
names one of them.

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
| `migrations/` | Ten migrations, embedded in `cmd/migrate`. |
| `deploy/` | Keycloak's realm, LocalStack's queues, and the observability stack's configuration. |
