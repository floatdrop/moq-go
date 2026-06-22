# syntax=docker/dockerfile:1
#
# Relay image for the moq-interop-runner (https://github.com/.../moq-interop-runner).
#
# Follows the runner's relay conventions: TLS material is mounted at
# /certs/cert.pem + /certs/priv.key, the listen port comes from $MOQT_PORT
# (default 4443), and the container exits 0 on SIGTERM. The entrypoint maps the
# runner's MOQT_* environment variables onto the relay's CLI flags, including
# MOQT_TRANSPORT to switch between raw QUIC (moqt://) and WebTransport (https://).
#
# Build from the repository root:
#   docker build -f cmd/relay/Dockerfile -t moq-relay:latest .
# or via the interop compose / Makefile target:
#   make interop-build

FROM golang:1.26-bookworm AS builder

WORKDIR /src

# Cache module downloads across source changes.
COPY go.mod go.sum ./
RUN go mod download

# Build only the relay command. The apps/tlmst sub-module is a separate module
# and is ignored by the root build (and excluded via .dockerignore anyway).
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/relay ./cmd/relay

FROM debian:bookworm-slim AS runtime

# ca-certificates: TLS roots; iproute2: `ss` for the healthcheck.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        iproute2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /out/relay /app/relay
COPY cmd/relay/entrypoint-relay.sh /app/run_endpoint.sh
RUN chmod +x /app/run_endpoint.sh && mkdir -p /certs /mlog

# Defaults follow the runner conventions; override at `docker run`/compose time.
ENV MOQT_ROLE=relay \
    MOQT_PORT=4443 \
    MOQT_TRANSPORT=quic \
    MOQT_WEBTRANSPORT_PATH=/moq

EXPOSE 4443/udp

# The relay is healthy once it is bound to the UDP listen port.
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s \
    CMD sh -c 'ss -uln | grep -q ":${MOQT_PORT:-4443}" || exit 1'

RUN useradd -r -u 1000 -g nogroup moq
USER moq

ENTRYPOINT ["/app/run_endpoint.sh"]
