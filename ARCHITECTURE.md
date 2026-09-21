# Architecture

## The HTTP API

`internal/adapters/http` (package `httpapi`) is the HTTP side of the two
application services. It routes, parses, authenticates, and maps what the
application layer answers onto status codes and bodies. It decides no business
rule and parses no value a provider sent.

Every response carries `X-Correlation-Id` and `Content-Type: application/json`,
except a router redirect, which carries only `Location`.

### Response mapping

`internal/app` classifies every error it returns. The class is what this adapter
maps; nothing here re-derives what happened.

| `app.Class` | Status | Notes |
|---|---|---|
| `INVALID` | 400 Bad Request | A malformed submission. Nothing was persisted and the idempotency key is still free. |
| `UNAUTHORIZED` | 401 / **403** | 401 when no principal was established (the credential was refused); **403** when one was and it may not do this. The application layer cannot tell these apart — it never sees a request that failed to authenticate. |
| `NOT_FOUND` | 404 Not Found | Also the answer for a provider reading another provider's operation, byte for byte. |
| `CONFLICT` | 409 Conflict | Covers both `WALLET_ALREADY_EXISTS` and `IDEMPOTENCY_PAYLOAD_CONFLICT` by one rule rather than two special cases. |
| `AUDIT` | 500 Internal Server Error | A finding about this service's stored records. The caller has nothing to correct and nothing to retry; the finding keeps its own code. |
| `RETRYABLE` | 503 Service Unavailable | Carries `Retry-After: 1`. |
| `UNRETRYABLE` | 500 Internal Server Error | Also where an error nobody classified goes. |
| `REJECTED` | — | Never arrives on an error. See below. |

Four statuses come from the transport rather than from a class, because no
business rule was consulted: **405** with `Allow` for a method a known path does
not answer, **413** for a body refused before it was read, **404** for a path no
route answers, and **307** with `Location` for a path the router can clean.

#### A submitted operation

A rejection is an outcome, not an error: it arrives on a **nil** error with an
`app.OperationResult` whose `Status` is `REJECTED` (ADR-0012). The status of a
submission therefore comes from the result.

| `OperationResult.Status` | Status |
|---|---|
| `PROCESSED` | 200 OK, with `idempotentReplay` set from the result |
| `PENDING_REFERENCE` | 202 Accepted |
| `REJECTED` | 422 Unprocessable Content |
| `PENDING`, `FAILED` | 200 OK |

**A read is always 200.** `GET /wagering/transactions/{transactionId}` and
`GET /providers/{providerId}/wagering/transactions/{externalTransactionId}`
answer 200 whatever the operation came to: the read succeeded, and the `status`
member of the body says what was read. Reusing the submission mapping would mean
answering 422 — "I could not process your request" — to a request that was
processed perfectly, and 202 — "accepted for processing" — where nothing was
accepted and nothing is being processed.

### The error body

Every refusal has the same three members, always all three:

```json
{"code": "…", "message": "…", "correlationId": "…"}
```

- **`code`** is the `failure.Code` when the failure names a catalogued reason,
  and the `app.Class` when it does not. Not every refusal has a code, and
  inventing one for those would put entries in the external contract describing
  nothing a provider can act on, while leaving the member every caller parses
  sometimes absent. The three transport codes above (`NOT_FOUND`,
  `METHOD_NOT_ALLOWED`, `CONTENT_TOO_LARGE`) are the only additions.
- **`message`** is the failure's own words for the classes a caller can act on
  (`INVALID`, `CONFLICT`, `NOT_FOUND`, `UNAUTHORIZED`), with the offending field
  prefixed when the failure names one. Everything else answers with a fixed
  sentence, because its message comes from infrastructure and would carry
  whatever a driver, a socket or a query put in it.
- **`correlationId`** is the thread this request was handled under.

An error's cause chain is never rendered, in any form. `app.ErrForeignOperation`
and the refusals in `internal/adapters/oidc` both keep a distinction an operator
needs *inside* the error chain precisely because a rendered message does not
show it. Rendering the chain would publish both distinctions and turn a scoped
read back into the existence oracle it was built to deny.

### Correlation

`X-Correlation-Id` is accepted, echoed on every response, and propagated to the
use case, where it stamps the outbox envelopes and every log line. A request
that carries none is given one.

A supplied value is held to `wagering.opaque_id`'s shape — the column it is
eventually stored in — and what happens when it does not fit depends on what is
wrong with it:

- A control character, invalid UTF-8, or surrounding whitespace is **refused**
  with 400. The value is written into every log line the request produces and
  echoed in a response header, so a caller that could put a newline in it would
  be writing those log lines; quietly repairing that would silence exactly the
  case somebody needs to hear about.
- A value longer than 128 bytes is **replaced** and the request goes on. That
  bound is a storage fact this service never published, and failing a
  well-formed identifier against a number the caller cannot know would fail a
  good request. The replacement is logged and the minted value is echoed.

### The endpoints

Every route but the two health checks requires a bearer token. A request refused
for its credential never reaches the application layer.

---

#### `POST /wagering/transactions` — role `provider`, `Idempotency-Key` required

The key is read from the header and nowhere else, is never trimmed, and is never
computed from the body. Deriving one would make every distinct payload its own
key, which is the opposite of what the header is for.

The member names are exactly the ones `wagering.CanonicalPayload` hashes, so the
bytes a provider sends, the bytes that are hashed and the bytes that come back
are one vocabulary. `referenceExternalTransactionId` is optional.

```json
{"provider":"acme","externalTransactionId":"acme-tx-1","playerId":"player-1",
 "roundId":"round-1","gameId":"game-1","kind":"BET",
 "money":{"amount":"25.00","currency":"BRL"}}
```

**200 — processed**

```json
{"transactionId":"0199aa00-0000-7000-8000-000000000002","externalTransactionId":"acme-tx-1","kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},"balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}
```

**200 — replay.** The same submission arriving again answers what the first one
answered, including the balance observed then rather than the wallet's balance
now.

```json
{"transactionId":"0199aa00-0000-7000-8000-000000000002","externalTransactionId":"acme-tx-1","kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},"balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":true}
```

**202 — waiting for its reference**

```json
{"transactionId":"0199aa00-0000-7000-8000-000000000002","externalTransactionId":"acme-tx-2","kind":"REFUND","status":"PENDING_REFERENCE","money":{"amount":"25.00","currency":"BRL"},"idempotentReplay":false}
```

**422 — rejected by a business rule.** Persisted, published, and the idempotency
key is bound to this payload for good.

```json
{"transactionId":"0199aa00-0000-7000-8000-000000000002","externalTransactionId":"acme-tx-3","kind":"BET","status":"REJECTED","money":{"amount":"25.00","currency":"BRL"},"failureCode":"INSUFFICIENT_FUNDS","idempotentReplay":false}
```

**400 — validation**

```json
{"code":"INVALID_AMOUNT_SCALE","message":"money: must carry exactly two fraction digits, got \"25.0\"","correlationId":"01a0c512-b9b1-74d4-a232-7371b01dccc8"}
```

**400 — a body this endpoint cannot read.** Unknown members, a member named
twice, a value that is not an object and a second value after the first are all
refused rather than resolved.

```json
{"code":"INVALID_FIELD_FORMAT","message":"body: the request body names a field this endpoint does not have","correlationId":"01a0c512-b9b1-778d-8751-f1a7e3c13654"}
```

The other two: `body: the request body names the same field twice` and
`body: the request body is not a JSON object this endpoint can read`.

**400 — `OPENING` submitted as a kind.** The handler passes the kind through
untouched; `wagering.Command.Validate` refuses it before any transaction opens.

```json
{"code":"UNSUPPORTED_TRANSACTION_KIND","message":"kind: OPENING is raised only when a wallet is opened","correlationId":"01a0c513-79c5-70b4-a350-4858d3c449c2"}
```

**400 — no `Idempotency-Key`**

```json
{"code":"MISSING_REQUIRED_FIELD","message":"Idempotency-Key: a submission must carry the provider's key for it","correlationId":"01a0c512-b9b1-7887-bc02-98d2321340d3"}
```

**401 — no usable credential.** With `WWW-Authenticate: Bearer realm="wagering"`.
One sentence whichever of the five credential refusals happened: which one it
was is logged, never answered.

```json
{"code":"UNAUTHORIZED","message":"the request carries no usable credential","correlationId":"01a0c512-b9b1-7981-aede-2cfb9e9cd0ee"}
```

**409 — the key is bound to another payload**

```json
{"code":"IDEMPOTENCY_PAYLOAD_CONFLICT","message":"idempotency key \"acme-key-1\" is already bound to another operation","correlationId":"01a0c512-b9b1-7a7e-a26a-52633f063ab0"}
```

**503 — temporarily unable.** With `Retry-After: 1`.

```json
{"code":"RETRYABLE","message":"the service is temporarily unable to answer, try again","correlationId":"01a0c512-b9b1-7b5c-929d-7f6e627b05f7"}
```

**500 — permanently unable**

```json
{"code":"UNRETRYABLE","message":"the request could not be completed","correlationId":"01a0c512-b9b1-7c56-8017-6c09944f9a98"}
```

**413 — the body was larger than this service accepts**

```json
{"code":"CONTENT_TOO_LARGE","message":"the request body is larger than this service accepts","correlationId":"01a0c512-b9b3-7018-a756-56ecd6e25293"}
```

---

#### `GET /wagering/transactions/{transactionId}` — a provider sees its own, `internal` sees all

**200 — whatever the operation came to**

```json
{"transactionId":"0199aa00-0000-7000-8000-000000000002","externalTransactionId":"acme-tx-1","kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},"balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}
```

```json
{"transactionId":"0199aa00-0000-7000-8000-000000000002","externalTransactionId":"acme-tx-3","kind":"BET","status":"REJECTED","money":{"amount":"25.00","currency":"BRL"},"failureCode":"INSUFFICIENT_FUNDS","idempotentReplay":false}
```

**404 — absent, or belonging to another provider.** The two answers are
identical, byte for byte and header for header: the application layer builds
both from the same sentence and keeps the difference in the error chain, where
`errors.Is` finds it and a rendered message does not.

```json
{"code":"NOT_FOUND","message":"no operation \"acme-tx-1\"","correlationId":"01a0c512-b9b1-7ec4-b8aa-c4852b7e7e37"}
```

A refusal that carries no message of its own falls back to the class's fixed
sentence, `there is no such resource`.

**400 — an identifier that is not one**

```json
{"code":"INVALID_FIELD_FORMAT","message":"transactionId: \"not-a-uuid\" is not a UUID","correlationId":"01a0c513-79c5-7b1a-80f9-cea2d773cbb0"}
```

---

#### `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}`

The provider comes from the path because an external id names an operation only
within the provider that issued it. `app.Principal.MayReadAs` decides whether
this caller may read as that provider: a provider may read only as itself, the
service may read as any.

**200** — the same operation view as above, for a provider naming itself or for
`internal` naming any provider.

**403 — a provider naming another provider.** Answered from the token and the
path alone, before anything is looked for, so it is the same answer for every
external id and confirms the existence of none of them. (404 would instead say
the operation is absent, which this route never went to find out.)

```json
{"code":"UNAUTHORIZED","message":"provider acme may not read as \"rival\"","correlationId":"01a0c512-b9b2-70c8-a383-1cdb047d05b5"}
```

**400 — a provider that is not a well-formed identifier**

```json
{"code":"INVALID_FIELD_FORMAT","message":"provider: must not be surrounded by whitespace","correlationId":"01a0c513-79c5-7da1-a440-fd274d36b4b2"}
```

---

#### `POST /wallets` — role `internal`

```json
{"playerId":"player-1","initialBalance":{"amount":"25.00","currency":"BRL"}}
```

`initialBalance` is required and names the currency, because a wallet is a
player's balance in one currency. A wallet with nothing in it is opened with
`"0.00"`.

**201 Created**, with `Location: /wallets/0199aa00-0000-7000-8000-000000000001`

```json
{"walletId":"0199aa00-0000-7000-8000-000000000001","playerId":"player-1","balance":{"amount":"25.00","currency":"BRL"},"version":1,"createdAt":"2026-09-21T12:00:00Z","updatedAt":"2026-09-21T12:00:00Z","opening":{"transactionId":"0199aa00-0000-7000-8000-000000000002","kind":"OPENING","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},"balance":{"amount":"25.00","currency":"BRL"},"idempotentReplay":false}}
```

`opening` is absent for a wallet opened at `"0.00"`: an opening records a
starting balance, and a wallet opened at zero has none.

**409 — the player already holds a wallet in that currency**

```json
{"code":"WALLET_ALREADY_EXISTS","message":"player \"player-1\" already holds a BRL wallet","correlationId":"01a0c512-b9b2-74b4-89e3-fd72c1b7cb18"}
```

---

#### `GET /wallets/{walletId}` — role `internal`

**200**

```json
{"walletId":"0199aa00-0000-7000-8000-000000000001","playerId":"player-1","balance":{"amount":"25.00","currency":"BRL"},"version":1,"createdAt":"2026-09-21T12:00:00Z","updatedAt":"2026-09-21T12:00:00Z"}
```

**403 — a provider asking**

```json
{"code":"UNAUTHORIZED","message":"provider acme may not administer wallets","correlationId":"01a0c512-b9b2-76e5-87cf-56927d5f6ed2"}
```

**404 — no such wallet**

```json
{"code":"NOT_FOUND","message":"no wallet 0199aa00-0000-7000-8000-000000000001","correlationId":"…"}
```

**400 — an identifier that is not one**

```json
{"code":"INVALID_FIELD_FORMAT","message":"walletId: \"not-a-uuid\" is not a UUID","correlationId":"01a0c513-79c5-7c6e-b299-0c8191cfdcf7"}
```

---

#### `GET /wallets/{walletId}/ledger?cursor=&limit=` — role `internal`

Oldest first, ordered by wallet version. The cursor is opaque and is carried
through untouched; the application layer decodes it and refuses one belonging to
another wallet. An absent `limit` is passed as unspecified so the application
layer's default applies; a `limit` that is not a whole number is 400.

**200**

```json
{"walletId":"0199aa00-0000-7000-8000-000000000001","entries":[{"ledgerEntryId":"0199aa00-0000-7000-8000-000000000003","transactionId":"0199aa00-0000-7000-8000-000000000002","direction":"CREDIT","money":{"amount":"25.00","currency":"BRL"},"balanceBefore":{"amount":"0.00","currency":"BRL"},"balanceAfter":{"amount":"25.00","currency":"BRL"},"walletVersion":1,"createdAt":"2026-09-21T12:00:00Z"}],"nextCursor":"MDE5OWFhMDAtMDAwMC03MDAwLTgwMDAtMDAwMDAwMDAwMDAxOjE"}
```

`nextCursor` is absent on the last page; its absence is how a caller knows to
stop. `entries` is `[]` on an empty page, never `null`.

**400 — a limit that is not a number**

```json
{"code":"INVALID_FIELD_FORMAT","message":"limit: must be a whole number","correlationId":"…"}
```

---

#### `POST /wallets/{walletId}/reconciliation` — role `internal`

**200 — whether or not the wallet balances.** The check ran and the report says
what it found; reconciliation never corrects anything. `difference` is stored
less reconstructed and may be negative.

```json
{"walletId":"0199aa00-0000-7000-8000-000000000001","consistent":false,"stored":{"amount":"20.00","currency":"BRL"},"reconstructed":{"amount":"25.00","currency":"BRL"},"difference":{"amount":"-5.00","currency":"BRL"}}
```

**500 — an audit finding.** The stored records are wrong in a way that is not a
mere imbalance. The finding keeps its own code.

```json
{"code":"LEDGER_BALANCE_MISMATCH","message":"the stored records for this wallet disagree and need operator attention","correlationId":"01a0c512-b9b2-7a8f-aa69-8cab89c842eb"}
```

---

#### `GET /health/live` — public

**200.** Reports on the process and asks nothing of anything else: a liveness
probe that checked a dependency restarts a healthy process whenever that
dependency is down, and does it to every replica at once.

```json
{"status":"alive"}
```

#### `GET /health/ready` — public

The checks run at the same time under one total budget, and every one of them
runs, so the body names them all. A failing check's error is logged and never
published: a readiness endpoint is usually reachable by more of a network than
the service is.

**200**

```json
{"status":"ready","checks":{"postgres":"ok","sqs":"ok"}}
```

**503**, with `Retry-After: 1`

```json
{"status":"unready","checks":{"postgres":"ok","sqs":"failed"}}
```

---

#### Router responses

**404 — no route answers this path**

```json
{"code":"NOT_FOUND","message":"no route answers this path","correlationId":"01a0c512-b9b2-7e9f-804e-b44f887e8770"}
```

**405**, with `Allow: POST`

```json
{"code":"METHOD_NOT_ALLOWED","message":"this path does not answer GET","correlationId":"01a0c512-b9b2-7f4f-9a40-cc6edcbaca57"}
```

**307**, with `Location: /wallets` and no body, for a path the router can clean
(`//wallets`, `/wallets/x/../y`). 307 preserves the method, so a submission
survives it; the HTML page the router would have drawn does not, because it is
the one representation this API never serves.

---

## Observability

`docker-compose.yml` brings up four more containers beside the service: an
OpenTelemetry Collector, Tempo, Prometheus and Grafana. Every API replica and
every worker exports over OTLP/gRPC to the collector and knows nothing else
about any of them — one address, `OTEL_EXPORTER_OTLP_ENDPOINT`. The collector
forwards traces to Tempo and holds metrics on `:8889`, which Prometheus scrapes
every five seconds. Their configuration is in `deploy/otel`, `deploy/tempo`,
`deploy/prometheus` and `deploy/grafana`.

That variable is **empty by default**, and empty means nothing is exported at
all: a process with no collector to talk to does not retry one. The compose
stack sets it; `.env.example` sets it to the same collector seen from the host.

| | Address | |
|---|---|---|
| Grafana | <http://localhost:3000> | the dashboard, already loaded |
| Prometheus | <http://localhost:9090> | |
| Tempo | <http://localhost:3200> | the search API below |
| Collector | `localhost:4317` gRPC, `4318` HTTP | what the service exports to |

### The dashboard

Grafana starts with both datasources and the dashboard **Wagering service**
provisioned from `deploy/grafana`. There is nothing to import and nothing to
configure. `make dashboards` prints the address and the credentials.

Those credentials are `GRAFANA_USER` and `GRAFANA_PASSWORD` in `.env.example` —
`admin` / `admin`, which are `docker-compose.yml`'s `GF_SECURITY_ADMIN_USER` and
`GF_SECURITY_ADMIN_PASSWORD`. They are placeholders in a repository, on a stack
reachable from nowhere but the laptop running it. **Reading the dashboard needs
no sign-in**: anonymous access is on, and the credentials are what anything that
writes has to present.

The panels, and the query behind each. Every selector below is
`{exported_job="wagering"}`, left out to keep the column readable:

| Panel | Query |
|---|---|
| Transaction outcomes | `sum by (status) (rate(wagering_transactions_total[$__rate_interval]))` |
| Outcomes since start | `sum by (status) (wagering_transactions_total)` |
| Rejections by failure code | `sum by (failureCode) (wagering_transactions_total{status="REJECTED"})` |
| Processing latency percentiles | `histogram_quantile(0.50 \| 0.95 \| 0.99, sum by (le) (rate(wagering_processing_duration_seconds_bucket[$__rate_interval])))` |
| Idempotent replays | `sum by (source, kind) (rate(wagering_idempotent_replays_total[$__rate_interval]))` |
| Inbox duplicates | `sum by (consumer) (rate(wagering_inbox_duplicates_total[$__rate_interval]))` |
| Lock conflicts | `sum by (transaction) (rate(wagering_lock_timeouts_total[$__rate_interval]))`, and the same over `wagering_version_conflicts_total` |
| Outbox lag | `max(wagering_outbox_lag_seconds)` |
| Outbox publish attempts | `sum by (outcome) (rate(wagering_outbox_publish_attempts_total[$__rate_interval]))` |
| SQS retries | `sum by (class) (rate(wagering_sqs_retries_total[$__rate_interval]))` |
| Dead letters | `sum(wagering_sqs_dead_letters_total) or vector(0)` |
| Reconciliation divergences | `sum(wagering_reconciliation_divergences_total) or vector(0)` |

The last two are the panels that must read zero. `or vector(0)` is there
because a counter that has never been incremented has no series at all, and a
panel that says "No data" where it should say "none" is a panel nobody believes
the second time.

**`exported_job`, not `job`.** The collector maps each service's `service.name`
onto a `job` label, so everything it exports arrives labelled `job="wagering"`.
That collides with the name of Prometheus's own scrape job, and Prometheus keeps
its own and renames the incoming one. Every query above therefore selects
`exported_job="wagering"`; `job="wagering"` matches nothing, silently. The same
series also carries `service_name="wagering"`, from the resource attribute the
collector copies onto each metric.

A rate over a counter that appeared once and never moved is zero — Prometheus
cannot tell a counter's first sample from a counter that was always at that
value. Three requests by hand therefore leave the rate panels flat; "Outcomes
since start" is the panel that shows them at all. Traffic spread over more than
one scrape interval fills the rest.

**The counters are one process's counters, not five.** Three API replicas and
two workers export a resource identified by `service.name` and nothing else, so
the collector cannot tell them apart and keeps one series where there should be
five. Three BETs, one to each replica, move `wagering_transactions_total` by
one. Traces are unaffected — a span carries its own identity. See
"Limitations"; again, no query here is what needs changing.

**The percentile panel reads coarse, and the cause is upstream of this
dashboard.** `wagering.processing.duration` is recorded in seconds and keeps the
OpenTelemetry SDK's default bucket boundaries, which begin at 5 and were chosen
for a duration in milliseconds. Every operation this service has performed falls
in the first bucket, so `histogram_quantile` interpolates inside (0s, 5s] and
answers 2.5 seconds for work whose exact mean — `_sum / _count`, which is not
bucketed — is six milliseconds. See "Limitations"; the query is not what needs
changing.

### Finding a trace by `correlationId`

Every span carries `correlationId`: the value `X-Correlation-Id` echoes, the
error body returns and every log line names. One trace spans the HTTP request,
the use case, the SQL transaction and the wallet events the outbox published
from it afterwards — the publish span is a child of the trace the event was
stored under, and is linked to the batch that carried it.

**In Grafana** — *Explore*, the **Tempo** datasource, the **TraceQL** tab:

```
{ .correlationId = "8d126071-1a25-4831-af62-7e61e80c59cb" }
```

The same query works on `.transactionId`, `.walletId`, `.providerId`,
`.messageId` and `.eventId`.

**From a shell**:

```
make trace CORRELATION=8d126071-1a25-4831-af62-7e61e80c59cb
```

which is Tempo's search API, and the whole of it:

```
curl --get --data-urlencode 'q={ .correlationId = "…" }' \
     --data "start=$(( $(date +%s) - 3600 ))" \
     --data "end=$(date +%s)" \
     http://localhost:3200/api/search
```

`start` and `end` are not optional. Outside its default window Tempo answers an
empty result rather than an error, so a trace that is not there and a trace that
is there but older look exactly alike. `GET /api/traces/<traceID>` then returns
the trace itself, in OTLP JSON — where the trace and span identifiers are
**base64**, not the hex the search result just printed.

One BET, found that way, is nineteen spans:

```
SPAN_KIND_SERVER    POST /wagering/transactions   correlationId=… kind=BET transactionId=…
SPAN_KIND_INTERNAL  Wagering.Submit
SPAN_KIND_CLIENT    postgres movement
SPAN_KIND_CLIENT    postgres.query                ×13
SPAN_KIND_PRODUCER  publish wallet event          eventId=…  → links to `publish outbox batch`
SPAN_KIND_PRODUCER  publish wallet event          eventId=…
```

The two producer spans are two seconds after the server span closed, in the
worker, in another container. They are in this trace because that is where the
event came from.

## Interpretations

The original challenge specification is not in this repository and could not be
recovered, so its section 9 — "contracts exactly as in the challenge spec" —
does not exist. The following were derived from the task text plus the
`internal/app` surface. Each names what was chosen and why.

1. **The HTTP package is `httpapi` in `internal/adapters/http`.** The directory
   is what the task specifies. The package is not called `http` because every
   file would then have to alias `net/http`, and `stdhttp.StatusOK` beside
   `http.StatusOK` in a package whose whole subject is HTTP is a rename waiting
   to be got wrong.

2. **`code` is the `failure.Code` when the failure carries one, and the
   `app.Class` when it does not.** Both are already published vocabularies. No
   code was invented for refusals that name none.

3. **`message` renders the failure's own words only for `INVALID`, `CONFLICT`,
   `NOT_FOUND` and `UNAUTHORIZED`.** The rest answer with a fixed sentence per
   class. The `app.Error` head — class, code, field — is stripped before the
   message is rendered, because the body carries the first two in a member of
   their own, and the field is put back in front of the sentence.

4. **The error body has exactly three members.** The offending field is prefixed
   to the message rather than added as a fourth, so that one shape stays one
   shape and a caller parsing a refusal never has to ask whether this one has
   the extra key.

5. **401 for a credential that was refused, 403 for a principal that may not do
   this.** `app.Unauthorized` covers both and the application layer cannot tell
   them apart, because it never sees a request that failed to authenticate.

6. **The 401 message is fixed.** Which of the five credential refusals happened
   is logged and never answered. Nothing is withheld that the holder of a token
   could not already read out of it; what is withheld is the one distinction
   they could not observe — a signature that did not check out and a key this
   process could not obtain.

7. **`AUDIT` is 500, keeping the finding's own code.** The finding is about this
   service's stored records: the caller has nothing to correct and nothing to
   retry, and repeating the request finds the same records.

8. **A submission's status says what it came to; a read is always 200.** The
   rule in the task text is stated over the outcome of a submission. Applying it
   to reads as well would answer 422 to a request that was processed perfectly
   and 202 where nothing was accepted, so any client with generic HTTP error
   handling would treat a successful read as a failure. The branch a provider
   needs — `status` in the body — is there either way.

9. **`POST /wallets` answers 201 with `Location`.** The statuses the task
   enumerates describe what a submitted *operation* came to, and opening a
   wallet is not one of them; 201 is what a POST that creates an addressable
   resource returns. The 409 in that same list is the conflict half of the same
   story and is answered the way every conflict here is.

10. **`POST /wallets/{id}/reconciliation` answers 200 even when the wallet does
    not balance.** The check ran; the report is the answer. It is a POST because
    the reconciliation is being produced, and because a check that reads every
    entry a wallet has is not something a caching intermediary should be invited
    to repeat on its own.

11. **Three transport codes exist beside the catalogue** — `NOT_FOUND`,
    `METHOD_NOT_ALLOWED`, `CONTENT_TOO_LARGE` — with **413** for an oversized
    body and **405 with `Allow`** for a method mismatch. Neither has an
    application class behind it: nothing was submitted, so nothing was invalid,
    rejected or in conflict.

12. **The router's own responses are intercepted and restated.** `ServeMux`
    composes three answers of its own — no route, a method mismatch, and a
    redirect for a path it can clean. A catch-all `/` was rejected as the fix:
    a pattern with no method matches every method, so it would capture the miss
    and turn every method mismatch into one too. The two refusals are rewritten
    in the contract's shape; the redirect keeps its status and `Location` and
    loses its HTML body.

13. **A supplied `X-Correlation-Id` is refused when it is dangerous and replaced
    when it is merely too long.** Control characters, invalid UTF-8 and
    surrounding whitespace are 400: the value is written into every log line the
    request produces, so quietly repairing it would silence a log-injection
    attempt. Over 128 bytes is replaced and logged: that bound is
    `wagering.opaque_id`'s and this service never published it, so failing a
    well-formed identifier against it would fail a good request over a storage
    fact.

14. **`OPENING` is refused by the domain, not by the handler.** The kind is
    passed through unmodified and `wagering.Command.Validate` — reached through
    `PayloadHash`, before any I/O — answers `UNSUPPORTED_TRANSACTION_KIND`.
    Duplicating the rule in the transport could only drift from it.

15. **The submission body carries `provider`.** The handler does not fill it
    from the token, so `app.Principal.MaySubmitAs` remains a live check rather
    than one trivially satisfied by the transport.

16. **`app.Principal.MayReadAs` was added to `internal/app`, and
    `Wagering.TransactionByExternalID` now takes the provider it reads as.**
    This is the one change made to the application layer, and it was made
    because the task requires `internal` to read all three of the wagering
    routes and the layer could not answer the by-external-id one: it derived the
    provider from the principal, so the service — which names no provider — was
    refused outright, and `TransactionByID` does not rescue the case because it
    needs a transaction id, which is exactly what a caller holding an external
    id has not got. The port underneath (`TransactionReader.ByExternal`) already
    took the provider explicitly and the route already carried it in the path.
    `MayReadAs` is deliberately not `MaySubmitAs`: a provider reads and submits
    only as itself, but the service reads as any provider although it submits as
    none, because reading takes nothing and moves nothing. A provider naming
    another provider is refused before any row is looked for, so the scoping
    guarantee is unchanged. Nothing else in `internal/app` was touched.

17. **Money crosses the wire through one view built from `money.Money`'s own
    rendering**, never by marshalling `money.Money` itself. The reconciliation
    difference is the system's one signed amount and `money.Money` refuses to
    marshal a negative, so one amount on this contract cannot be written by the
    type that writes the others — and a package with two ways to write an amount
    would eventually write that one the wrong way. No float appears on any path.

18. **`Idempotency-Key` is a header and never a body member.** An
    `idempotencyKey` in the body is an unknown member and is a 400. The header
    is never trimmed and never computed.

19. **`Retry-After` is the constant `1`.** A retryable failure here is a lost
    connection, a statement timeout or a lock conflict, all of which are over in
    milliseconds. A caller that needs a different pace has its own backoff.

20. **`net/http`'s `ServeMux` was chosen over a routing dependency.** Its
    method-and-wildcard patterns match every route this API has.

## Limitations

- **No panic recovery middleware.** `net/http` already contains a handler panic
  to the one connection it happened on, so the blast radius is bounded, but a
  panicking handler drops that connection with no correlated response and no
  entry in the error body's shape.
- **No access log.** Only refusals, failures and the distinctions kept off the
  wire are logged. A request log belongs to the composition root, which owns the
  logger.
- **`routed` does not forward `http.Flusher`, `http.Hijacker` or
  `io.ReaderFrom`.** Nothing in this API streams, upgrades or sends a file, and
  `http.ResponseController` reaches the writer underneath through `Unwrap`, but
  middleware written against the bare type assertions would silently do nothing.
- **The SQS readiness check does not exist yet.** `/health/ready` takes a set of
  named checks and the composition root supplies them; until the queue adapter
  exists, the set names only PostgreSQL.
- **`wagering.processing.duration` has millisecond-shaped buckets and
  second-shaped values.** The instrument is created with `metric.WithUnit("s")`
  and recorded with `Took.Seconds()`, but names no boundaries — so it takes the
  SDK's defaults, `0, 5, 10, 25 … 10000`, which are the defaults for a duration
  measured in milliseconds. Every observation lands in the first bucket. The sum
  and the count stay exact, and every percentile the dashboard can compute is an
  interpolation inside a single five-second bucket: p50 reads 2.5s where the
  mean is 0.006s. The fix is one option on the instrument in
  `internal/telemetry/metrics.go` —
  `metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.075, 0.1,
  0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10)`, the boundary set the OpenTelemetry
  semantic conventions give for a duration in seconds. The dashboard needs no
  change when it lands.
- **Only one process's measurements reach the dashboard.** Every process builds
  its OpenTelemetry resource from `service.name` alone — `resource.Default()`
  plus that one attribute, with no `service.instance.id`. The collector's
  Prometheus exporter keys a series by its labels, and five processes reporting
  the same metric under the same labels are one series to it: three BETs, one to
  each API replica, move `wagering_transactions_total` by one, and the other two
  are lost with no error anywhere. The fix is one attribute on the resource in
  `internal/fxmod/telemetry.go` — `semconv.ServiceInstanceID` from the hostname,
  which the container runtime already sets to something distinct per replica and
  which the exporter maps to `instance`. Every query on the dashboard already
  aggregates with `sum by (...)`, so none of them changes when it lands. Traces
  never had this problem: a span identifies itself.
