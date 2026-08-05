# moq — build, test, and interop targets.
#
# The interop-* targets run a third-party MoQT test client (from the
# moq-interop-runner registry) against the relay built from this repository,
# exercising both raw QUIC (moqt://) and WebTransport (https://) transports.

.PHONY: build test test-race lint \
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

lint:
	golangci-lint run

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
