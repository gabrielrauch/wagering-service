# Wagering service.
#
# DATABASE_URL is the connection the migration commands use. It must connect as
# a role that is a member of wagering_migrator; the service itself runs as a
# member of wagering_app, which has no DDL. See docs/schema.md.
DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/wagering?sslmode=disable

# How long `make fuzz` spends on each parser. The seed corpus runs as part of
# `make test`; this is the part that goes looking for inputs nobody wrote down.
FUZZTIME ?= 1m

.PHONY: help
help: ## List the targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-16s %s\n", $$1, $$2}'

.PHONY: test
test: ## Run every test, with the race detector
	go test -race ./...

.PHONY: fmt
fmt: ## Report what gofmt would change
	gofmt -l .

.PHONY: vet
vet: ## Report what go vet finds
	go vet ./...

.PHONY: lint
lint: ## Report what golangci-lint finds
	golangci-lint run

.PHONY: check
check: fmt vet lint test ## Run the checks a commit has to pass

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

.PHONY: migrate-up
migrate-up: ## Apply every migration not yet applied
	go run ./cmd/migrate -database "$(DATABASE_URL)" up

.PHONY: migrate-down
migrate-down: ## Revert every applied migration
	go run ./cmd/migrate -database "$(DATABASE_URL)" down

.PHONY: migrate-version
migrate-version: ## Report the applied schema version
	go run ./cmd/migrate -database "$(DATABASE_URL)" version

.PHONY: db-up
db-up: ## Start a local PostgreSQL 16 to migrate against
	docker run --rm -d --name wagering-db \
		-e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=wagering \
		-p 5432:5432 postgres:16-alpine

.PHONY: db-down
db-down: ## Stop the local PostgreSQL
	docker rm -f wagering-db
