# The collector, plus the one binary needed to ask it whether it is up.
#
# otel/opentelemetry-collector-contrib is a scratch image: one 300MB binary, a
# CA bundle, and nothing else — no shell, no HTTP client. Compose can only run
# a healthcheck inside the container it is checking, so an image with nothing
# in it is an image whose health endpoint nothing can ask, and `otel-collector:
# {condition: service_healthy}` could not be written at all.
#
# busybox is taken from the Prometheus image rather than installed, because
# there is no package manager here to install it with and because that image is
# already pinned and already pulled by this stack — it costs no download and no
# new version to keep track of. It is statically linked, which is what lets it
# run on a base with no libc.
#
# It lands under the name `wget` on purpose: busybox dispatches on argv[0], so a
# binary called `wget` is a wget and cannot be talked into being a shell by
# anything that cannot already choose the argv it execs with. The service
# images do the same thing for the same reason — see the Dockerfile at the root.
FROM otel/opentelemetry-collector-contrib:0.140.0
COPY --from=prom/prometheus:v3.9.0 /bin/busybox /usr/bin/wget
