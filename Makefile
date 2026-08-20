# moq — build, test, and interop targets.
#
# The interop-* targets run a third-party MoQT test client (from the
# moq-interop-runner registry) against the relay built from this repository,
# exercising both raw QUIC (moqt://) and WebTransport (https://) transports.

.PHONY: build test test-race test-submodules test-all lint \
        bench bench-quick bench-smoke bench-baseline \
        cover cover-gaps cover-html cover-total cover-site \
        interop interop-quic interop-webtransport interop-matrix interop-build interop-certs interop-clean \
        interop-client interop-client-build

# ---------------------------------------------------------------------------
# Standard Go targets
# ---------------------------------------------------------------------------

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

# pkg/relay/discovery/etcd is a separate Go module — `go test ./...` from the
# root does not reach it, so `make test` alone says nothing about the discovery
# backend or the relay-etcd binary. Its tests are hermetic (an embedded
# in-process etcd server); no daemons required. The list is plural because a
# second backend module has lived here before and may again.
SUBMODULES := pkg/relay/discovery/etcd

test-submodules:
	@for m in $(SUBMODULES); do \
		echo ">> $$m"; \
		(cd $$m && go build ./... && go test -race ./...) || exit 1; \
	done

# Everything the root suite covers, plus the submodules it cannot see.
test-all: test test-submodules

lint:
	golangci-lint run

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------
#
# Two tiers, because the full suite is too slow to run after every slice of
# work. See benchmarks/README.md for what each one is and is not evidence of.
#
#   bench-quick  ~5s    allocs/op and B/op only — the per-change guard
#   bench        ~4m30s allocs/op, B/op AND ns/op — feeds benchstat
#
# The throughput probes are excluded from both: they are "how fast can it go"
# measurements, not regression signals.
BENCH_SKIP  := FanoutBuffered|FanoutQUIC
BENCH_PKGS  ?= ./pkg/...
# Fixed iteration counts, not a duration. allocs/op is deterministic per
# iteration, so a short fixed run reproduces it exactly while costing ~1s per
# package; 500x is where the relay fanout benchmarks amortize their per-run
# setup and converge on the same counts the full suite reports.
BENCH_QUICK_TIME := 500x

# The per-change guard. ns/op from this target is MEANINGLESS — 500 iterations
# is far too few to amortize setup or warm caches, and Fanout1to1 reads 2-3x
# slower here than it truly is. Read allocs/op and B/op; use `make bench` for
# anything about time.
bench-quick:
	go test -run='^$$' -bench=. -skip='$(BENCH_SKIP)' -benchmem \
		-benchtime=$(BENCH_QUICK_TIME) -count=1 $(BENCH_PKGS) 2>/dev/null

# The full regression suite. -count=10 gives benchstat enough samples for a
# stable mean and variance; redirect to a file and compare two of them.
bench:
	go test -run='^$$' -bench=. -skip='$(BENCH_SKIP)' -benchmem \
		-count=10 $(BENCH_PKGS) 2>/dev/null

# Compile-and-run check only, one iteration each — catches a benchmark that no
# longer builds or panics. Includes the throughput probes deliberately; they
# bit-rot too. This is the CI job.
bench-smoke:
	go test -run='^$$' -bench=. -benchtime=1x -count=1 ./... >/dev/null

# Refresh the committed reference run. Deliberate, never silent: do this only
# when an intentional performance change lands, and say so in the commit.
bench-baseline:
	go test -run='^$$' -bench=. -skip='$(BENCH_SKIP)' -benchmem -count=10 \
		./pkg/moqt/wire/ ./pkg/moqt/message/ ./pkg/moqt/session/ ./pkg/relay/ ./pkg/relay/cache/ \
		> benchmarks/baseline-go1.27.txt 2>/dev/null

# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------

COVER_PROFILE ?= coverage.out

# Where `cover-site` writes the report CI publishes. Gitignored.
COVER_SITE ?= site

# Scoped to the library by default. The cmd/* binaries are thin wiring over
# these packages and are covered end-to-end by the interop suite instead, so
# including them buries the actionable gaps under ~250 lines of 0.0% main().
# Override to see everything: make cover COVER_PKGS=./...
COVER_PKGS ?= ./pkg/...

# Per-package summary, each package credited only for what its OWN tests reach.
# The quickest local read; `cover-total` is the published view.
cover:
	go test -cover $(COVER_PKGS)

# Every function the tests never reach, least-covered first. Untested code is
# either a missing test or dead code; this is the list that decides which.
cover-gaps:
	@go test -coverprofile=$(COVER_PROFILE) $(COVER_PKGS) >/dev/null
	@go tool cover -func=$(COVER_PROFILE) | grep -v '100.0%' | sort -k3 -n

cover-html:
	@go test -coverprofile=$(COVER_PROFILE) $(COVER_PKGS) >/dev/null
	go tool cover -html=$(COVER_PROFILE)

# Whole-suite coverage (-coverpkg) across BOTH modules — the root and
# pkg/relay/discovery/etcd. Credits a package for the code any test reaches,
# which is the figure the README badge and the published report show. Nothing
# gates on it; see the script header.
cover-total:
	@./scripts/coverage-report.sh

# The same measurement, plus the report site CI publishes to GitHub Pages:
# a per-package index, a line-by-line HTML report per module, and the shields
# endpoint JSON the badge reads. Open $(COVER_SITE)/index.html to preview
# exactly what will be published.
cover-site:
	@./scripts/coverage-report.sh --site $(COVER_SITE)

# ---------------------------------------------------------------------------
# Interop testing
# ---------------------------------------------------------------------------

# Image tags / client are overridable, e.g.
#   make interop CLIENT_IMAGE=ghcr.io/englishm/moq-interop-runner-moq-test-client-draft-18:latest
RELAY_IMAGE  ?= moq-relay:latest
CLIENT_IMAGE ?= ghcr.io/englishm/moq-interop-runner-moq-dev-rs-client:latest
COMPOSE      := docker compose -f interop/docker-compose.yml
COMPOSE_ENV  := RELAY_IMAGE=$(RELAY_IMAGE) CLIENT_IMAGE=$(CLIENT_IMAGE)

# TESTCASE / VERBOSE are forwarded to the client only when set, e.g.
#   make interop-quic TESTCASE=setup-only VERBOSE=true
CLIENT_ENV   := $(if $(TESTCASE),TESTCASE=$(TESTCASE)) $(if $(VERBOSE),VERBOSE=$(VERBOSE))

# Build the relay image from this repo's sources.
interop-build:
	docker build -f cmd/relay/Dockerfile -t $(RELAY_IMAGE) .

# Generate the self-signed cert pair the relay serves.
interop-certs:
	@./interop/generate-certs.sh ./interop/certs

# Run the full interop suite over both transports.
interop: interop-quic interop-webtransport

interop-quic: interop-certs
	@echo ">> interop: raw QUIC (moqt://relay:4443)"
	$(COMPOSE_ENV) $(CLIENT_ENV) MOQT_TRANSPORT=quic RELAY_URL=moqt://relay:4443 \
		$(COMPOSE) up --build --abort-on-container-exit --exit-code-from test-client; \
	status=$$?; $(COMPOSE) down -v >/dev/null 2>&1; exit $$status

interop-webtransport: interop-certs
	@echo ">> interop: WebTransport (https://relay:4443/moq)"
	$(COMPOSE_ENV) $(CLIENT_ENV) MOQT_TRANSPORT=webtransport RELAY_URL=https://relay:4443/moq \
		$(COMPOSE) up --build --abort-on-container-exit --exit-code-from test-client; \
	status=$$?; $(COMPOSE) down -v >/dev/null 2>&1; exit $$status

# Run the relay against several third-party draft-18 clients over the transports
# each supports, and print a pass/skip/fail matrix. Override the client set with
# the CLIENTS env var (see interop/matrix.sh).
interop-matrix: interop-build interop-certs
	@RELAY_IMAGE=$(RELAY_IMAGE) ./interop/matrix.sh

# ---------------------------------------------------------------------------
# Client-direction interop: OUR client against a third-party relay
# ---------------------------------------------------------------------------

# `make interop-client` defaults to a self-contained loopback (our client ↔ our
# relay) as an always-green regression guard. Override CLIENT_RELAY_IMAGE /
# CLIENT_RELAY_URL to test against a third-party relay, e.g.:
#   make interop-client CLIENT_RELAY_IMAGE=ghcr.io/englishm/moq-interop-runner-moq-relay-ietf-draft-18:latest
#   make interop-client CLIENT_RELAY_IMAGE=moq-dev-rs-interop:latest   # (build the adapter in the runner first)
# For our client against EVERY registered draft-18 relay, use the runner:
#   cd ../moq-interop-runner && make interop-client CLIENT=moq-go
CLIENT_RELAY_IMAGE ?= moq-relay:latest
CLIENT_RELAY_URL   ?= moqt://relay:4443
CLIENT_COMPOSE     := docker compose -f interop/docker-compose.client.yml

# Transport our loopback relay must speak, derived from the URL scheme so the
# default moqt:// loopback uses raw QUIC and an https:// override uses
# WebTransport. Only consumed by our own relay image (third-party relays ignore
# MOQT_TRANSPORT); see interop/docker-compose.client.yml.
CLIENT_RELAY_TRANSPORT ?= $(if $(filter https://%,$(CLIENT_RELAY_URL)),webtransport,quic)

# Build our test-client image from this repo's sources.
interop-client-build:
	docker build -f cmd/interop-client/Dockerfile.client -t moq-interop-client:latest .

# Run our client against a relay image. Builds our relay too so the default
# loopback works out of the box; harmless when targeting a third-party relay.
interop-client: interop-build interop-certs
	@echo ">> interop-client: moq-interop-client -> $(CLIENT_RELAY_IMAGE) ($(CLIENT_RELAY_URL))"
	RELAY_IMAGE=$(CLIENT_RELAY_IMAGE) RELAY_URL=$(CLIENT_RELAY_URL) \
		MOQT_TRANSPORT=$(CLIENT_RELAY_TRANSPORT) $(CLIENT_ENV) \
		$(CLIENT_COMPOSE) up --build --abort-on-container-exit --exit-code-from test-client; \
	status=$$?; $(CLIENT_COMPOSE) down -v >/dev/null 2>&1; exit $$status

# Our client against our own relay over BOTH mappings — one relay image, both URL
# schemes. Keeps WebTransport covered now that the third-party job (`make
# interop`) is manual-only: nothing else exercises the relay container's HTTP/3
# path, and a stale flag in entrypoint-relay.sh once broke it unnoticed.
interop-loopback:
	$(MAKE) interop-client CLIENT_RELAY_URL=moqt://relay:4443
	$(MAKE) interop-client CLIENT_RELAY_URL=https://relay:4443/

interop-clean:
	-$(COMPOSE) down -v >/dev/null 2>&1
	-$(CLIENT_COMPOSE) down -v >/dev/null 2>&1
	rm -rf ./interop/certs
