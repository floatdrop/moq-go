#!/bin/sh
# entrypoint-relay.sh — translate the moq-interop-runner's MOQT_* environment
# variables into `relay` CLI flags.
#
# Environment:
#   MOQT_ROLE              Role to run; only "relay" is supported (default: relay)
#   MOQT_PORT              UDP port to listen on (default: 4443)
#   MOQT_CERT              TLS certificate PEM (default: /certs/cert.pem)
#   MOQT_KEY               TLS private key PEM (default: /certs/priv.key)
#   MOQT_TRANSPORT         "quic" (moqt://) or "webtransport" (https://).
#                          Accepted and logged, but it no longer selects
#                          anything: the relay serves both mappings on one port,
#                          so a client picks by URL scheme. Kept because the
#                          runner sets it and rejecting an unknown value is a
#                          clearer failure than ignoring a typo.
#   MOQT_WEBTRANSPORT_PATH HTTP/3 CONNECT path for WebTransport (default: /)
#                          The runner dials a path-less https://relay:4443, so
#                          the CONNECT targets "/"; serving WebTransport at the
#                          root is what lets the upgrade succeed (otherwise the
#                          request 404s against the /moq handler).
#
# Mounts:
#   /certs/cert.pem, /certs/priv.key
#
# Exit codes: 0 clean shutdown, 1 config error, 127 unsupported role.

set -eu

ROLE="${MOQT_ROLE:-relay}"
PORT="${MOQT_PORT:-4443}"
CERT="${MOQT_CERT:-/certs/cert.pem}"
KEY="${MOQT_KEY:-/certs/priv.key}"
TRANSPORT="${MOQT_TRANSPORT:-webtransport}"  # informational; both are served
WT_PATH="${MOQT_WEBTRANSPORT_PATH:-/}"

if [ "$ROLE" != "relay" ]; then
    echo "role '$ROLE' not supported (only 'relay')" >&2
    exit 127
fi

if [ ! -f "$CERT" ]; then
    echo "ERROR: certificate not found at $CERT — mount /certs with cert.pem and priv.key" >&2
    exit 1
fi
if [ ! -f "$KEY" ]; then
    echo "ERROR: private key not found at $KEY" >&2
    exit 1
fi

# One port serves raw QUIC and WebTransport both, so there is no transport flag
# to set — only where the WebTransport CONNECT handler mounts.
set -- -addr "0.0.0.0:$PORT" -cert "$CERT" -key "$KEY" -webtransport-path "$WT_PATH"
case "$TRANSPORT" in
    quic|webtransport|wt)
        ;;
    *)
        echo "ERROR: MOQT_TRANSPORT must be 'quic' or 'webtransport' (got '$TRANSPORT')" >&2
        exit 1
        ;;
esac

echo "Starting moq relay: port=$PORT cert=$CERT wt-path=$WT_PATH" \
     "(serving raw QUIC and WebTransport; MOQT_TRANSPORT=$TRANSPORT is informational)" >&2
exec /app/relay "$@"
