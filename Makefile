# Wagering service.
#
# DATABASE_URL is the connection the migration commands use. It must connect as
# a role that is a member of wagering_migrator; the service itself runs as a
# member of wagering_app, which has no DDL. See docs/schema.md.
#
# The targets that talk to the local stack — token, dashboards, and the migrate
# ones — reach it through the ports docker-compose.yml publishes, so `make up`
# and then any of them is the whole loop.
DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/wagering?sslmode=disable

# How long `make fuzz` spends on each parser. The seed corpus runs as part of
# `make test`; this is the part that goes looking for inputs nobody wrote down.
FUZZTIME ?= 1m

# Where `make token` asks for a token, and which client it asks as. Every client
# in deploy/keycloak/realm-export.json has the secret `<clientId>-secret`, which
# is a placeholder and could not be anything else: the realm is imported from a
# file in the repository.
KEYCLOAK_URL ?= http://localhost:8080
REALM ?= wagering
CLIENT ?= provider-a
CLIENT_SECRET ?= $(CLIENT)-secret

# Where the observability stack publishes Grafana and Tempo. `make dashboards`
# prints the first three; `make trace` asks the fourth. All four are documented
# in .env.example beside the variables the service itself reads, and all four
# are taken from the environment when it sets them.
GRAFANA_URL ?= http://localhost:3000
GRAFANA_USER ?= admin
GRAFANA_PASSWORD ?= admin
TEMPO_URL ?= http://localhost:3200

.PHONY: help
help: ## List the targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-18s %s\n", $$1, $$2}'

.PHONY: test
test: ## Run every test
	go test ./...

.PHONY: test-race
test-race: ## Run every test with the race detector
	go test -race ./...

.PHONY: test-integration
test-integration: ## Run the suites that need real PostgreSQL, SQS and Keycloak
	go test -race -tags integration -count=1 ./...

# Nothing has to be started first: the suite brings the compose stack up itself
# and builds the two binaries it drives, which is idempotent and costs about two
# seconds against a stack that is already healthy. Budget three and a half
# minutes from cold and two and a half warm; internal/multi/doc.go breaks that
# down.
.PHONY: test-multi
test-multi: ## Run the suite that drives several replicas at once (~3 min, starts the stack)
	go test -race -tags multi -count=1 -timeout 20m ./...

.PHONY: fmt
fmt: ## Report what gofmt would change
	gofmt -l .

.PHONY: vet
vet: ## Report what go vet finds
	go vet ./...

.PHONY: lint
lint: ## Report what golangci-lint finds
	golangci-lint run

# test-race rather than test: the race detector is the part of this gate that
# finds what review does not, so the commit check keeps it even though `make
# test` is now the quick run.
.PHONY: check
check: fmt vet lint test-race ## Run the checks a commit has to pass

.PHONY: fuzz
fuzz: ## Fuzz the parsers that read provider input, for FUZZTIME each
	go test -run '^$$' -fuzz '^FuzzParse$$' -fuzztime=$(FUZZTIME) ./internal/domain/money/
	go test -run '^$$' -fuzz '^FuzzParseOpaque$$' -fuzztime=$(FUZZTIME) ./internal/domain/wagering/

.PHONY: vuln
vuln: ## Report known vulnerabilities in the dependency graph
	govulncheck ./...

.PHONY: cover
cover: ## Report test coverage per function
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

.PHONY: fix
fix: ## Apply the fixes gofmt, go fix and golangci-lint can make on their own
	go fix ./...
	# --fix exits non-zero when findings remain that it cannot repair, which is
	# not a failure of this target. `make lint` is what reports them.
	golangci-lint run --fix || true
	golangci-lint fmt

# --wait names services rather than being left to cover everything, because the
# migration job exits as soon as it has done its work and `--wait` reports a
# container that exited as a failure. The job still runs: every service named
# here depends on its completion.
#
# The five named pull in the other seven. The API replicas and the worker wait
# on the collector, which waits on Tempo; Grafana waits on Prometheus and on
# Tempo. Naming grafana is what makes this target return with a dashboard that
# opens, rather than one still forty seconds from answering — Grafana is by some
# distance the slowest thing here to start.
.PHONY: up
up: ## Build and start the whole stack, and wait for it to be healthy
	docker compose up --build --detach --wait api-1 api-2 api-3 worker grafana
	@docker compose ps

.PHONY: down
down: ## Stop the stack, keeping the database volume
	docker compose down --remove-orphans

.PHONY: clean
clean: ## Stop the stack and delete the database volume with it
	docker compose down --remove-orphans --volumes

.PHONY: logs
logs: ## Follow the logs of every service
	docker compose logs --follow

.PHONY: migrate-up
migrate-up: ## Apply every migration not yet applied
	go run ./cmd/migrate -database "$(DATABASE_URL)" up

.PHONY: migrate-down
migrate-down: ## Revert every applied migration
	go run ./cmd/migrate -database "$(DATABASE_URL)" down

.PHONY: migrate-version
migrate-version: ## Report the applied schema version
	go run ./cmd/migrate -database "$(DATABASE_URL)" version

# Prints the token and nothing else, so that it composes:
#
#   curl -H "Authorization: Bearer $$(make token CLIENT=provider-b)" ...
#
# The realm's clients are all confidential service accounts, so this is the
# client_credentials grant and there is no user in it. The token is extracted
# with sed rather than jq, which is not something a developer should have to
# install to call their own API.
.PHONY: token
token: ## Fetch a client_credentials token for CLIENT (default provider-a)
	@curl --fail --silent --show-error \
		--data grant_type=client_credentials \
		--data client_id=$(CLIENT) \
		--data client_secret=$(CLIENT_SECRET) \
		"$(KEYCLOAK_URL)/realms/$(REALM)/protocol/openid-connect/token" \
		| sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p'

.PHONY: dashboards
dashboards: ## Print where Grafana is and how to sign in
	@echo "Grafana:  $(GRAFANA_URL)"
	@echo "user:     $(GRAFANA_USER)"
	@echo "password: $(GRAFANA_PASSWORD)"
	@echo
	@echo 'Both datasources and the dashboard are provisioned from deploy/grafana,'
	@echo 'so "Wagering service" is loaded at start-up and there is nothing to'
	@echo 'import. Reading it needs no sign-in; the credentials are for writing.'
	@echo
	@echo 'The trace behind one correlationId — Explore, the Tempo datasource,'
	@echo 'the TraceQL tab:'
	@echo
	@echo '    { .correlationId = "<id>" }'
	@echo
	@echo 'or, without a browser:  make trace CORRELATION=<id>'

# Prints Tempo's answer as it comes, which is one JSON object listing every
# trace that carried this correlationId — its traceID, its root span and how
# long it took.
#
# start and end are not optional. Tempo searches a default window and answers
# an empty result, not an error, for anything outside it — so a trace that is
# not there and a trace that is there but older look exactly alike. This asks
# for the last hour, which is longer than this stack usually lives.
.PHONY: trace
trace: ## Find the trace for CORRELATION=<correlationId> in Tempo
	@test -n "$(CORRELATION)" || \
		{ echo 'usage: make trace CORRELATION=<correlationId>' >&2; exit 2; }
	@curl --fail --silent --show-error --get \
		--data-urlencode 'q={ .correlationId = "$(CORRELATION)" }' \
		--data "start=$$(( $$(date +%s) - 3600 ))" \
		--data "end=$$(date +%s)" \
		"$(TEMPO_URL)/api/search"
	@echo

.PHONY: db-up
db-up: ## Start a local PostgreSQL 16 to migrate against
	docker run --rm -d --name wagering-db \
		-e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=wagering \
		-p 5432:5432 postgres:16-alpine

.PHONY: db-down
db-down: ## Stop the local PostgreSQL
	docker rm -f wagering-db
