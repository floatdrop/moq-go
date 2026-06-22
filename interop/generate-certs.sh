#!/bin/sh
# Generate a self-signed TLS cert for local interop testing, matching the
# moq-interop-runner convention (/certs/cert.pem + /certs/priv.key).
#
# ECDSA P-256 with a short (10-day) validity so the cert also satisfies the
# WebTransport serverCertificateHashes policy (ECDSA key + <=14-day validity).
set -eu

CERTS_DIR="${1:-./interop/certs}"

if [ -f "$CERTS_DIR/cert.pem" ] && [ -f "$CERTS_DIR/priv.key" ]; then
    echo "certs already present in $CERTS_DIR (delete them to regenerate)"
    exit 0
fi

mkdir -p "$CERTS_DIR"

# ec_param_enc:named_curve forces the curve to be encoded as an OID rather than
# explicit parameters — LibreSSL (macOS) defaults to explicit params, which Go's
# crypto/x509 rejects with "invalid ECDSA parameters". No-op on OpenSSL/Linux.
openssl req -x509 -newkey ec \
    -pkeyopt ec_paramgen_curve:prime256v1 \
    -pkeyopt ec_param_enc:named_curve \
    -keyout "$CERTS_DIR/priv.key" \
    -out "$CERTS_DIR/cert.pem" \
    -days 10 -nodes \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,DNS:relay,DNS:moq-relay,IP:127.0.0.1"

# Ephemeral test certs (not secrets); 644 so a non-root container UID can read.
chmod 644 "$CERTS_DIR/priv.key" "$CERTS_DIR/cert.pem"

echo "Generated certificates in $CERTS_DIR"
