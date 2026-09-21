# The three images this service ships: the migration job, the API and the
# worker. They share one build stage, so the compiler runs once and the three
# targets differ only in which binary they carry.
#
# Build one with `docker build --target api .`; docker-compose.yml names all
# three.

# The Go version is declared here rather than inherited, so a toolchain that
# drifts is a build failure and not a surprise at run time. It must match the
# `go` directive in go.mod.
ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

# A statically linked busybox, which becomes the health probe of the images
# below. Distroless carries no shell and no HTTP client, and Compose can only
# run a healthcheck inside the container it is checking — so an image with
# nothing in it is an image whose readiness endpoint nothing can ask. It is
# installed in this stage and copied, never installed into the final image:
# there is no package manager there to install it with, which is the point.
RUN apk add --no-cache busybox-static

# Dependencies are a layer of their own, so a change to the source does not
# re-download the module graph. That graph is not small — pgx, the AWS SDK
# credential chain, jwx, fx and testcontainers are all in it.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# One invocation for all three commands rather than three, so they share a
# single type-check of the packages they have in common.
#
# CGO is off so the binaries are static and can run on a base with no libc at
# all. The migrations are embedded (migrations/embed.go), so nothing has to be
# copied beside the migrate binary and its image needs no writable filesystem.
#
# TARGETOS and TARGETARCH are set by buildx and left with defaults for a plain
# `docker build`, so a cross-build and a local build are the same command.
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-arm64} \
    go build -trimpath -ldflags='-s -w' -o /out/ ./cmd/migrate ./cmd/api ./cmd/worker

# distroless:nonroot carries a CA bundle, /etc/passwd and nothing else — no
# shell, no package manager, nothing for an attacker who lands inside to use.
# Each image below starts from it again rather than from a common intermediate,
# because a shared intermediate would put every binary in every image.

# The migration job. It runs to completion and exits; nothing probes it, so it
# gets no probe.
FROM gcr.io/distroless/static-debian12:nonroot AS migrate
COPY --from=build /out/migrate /usr/local/bin/migrate
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/migrate"]

# The API. busybox is installed under the name `wget` deliberately: busybox
# dispatches on argv[0], so a binary called `wget` is a wget and cannot be
# talked into being a shell by anything that cannot already choose the argv it
# execs with — which is everything this image can do, since the service execs
# nothing.
FROM gcr.io/distroless/static-debian12:nonroot AS api
COPY --from=build /bin/busybox.static /usr/bin/wget
COPY --from=build /out/api /usr/local/bin/api
USER nonroot:nonroot
# Documentation only — it publishes nothing. HTTP_ADDR is what the server binds,
# and this is the port docker-compose.yml sets it to.
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/api"]

# The worker. It serves nothing, so it has nothing to probe and carries no
# probe: a readiness endpoint is the API's, and a check that only proved PID 1
# was alive would say exactly what the container's own state already says. The
# worker's start-up is its check — it pings the database and resolves its queues
# before it reports started, and exits non-zero if it cannot.
FROM gcr.io/distroless/static-debian12:nonroot AS worker
COPY --from=build /out/worker /usr/local/bin/worker
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/worker"]
