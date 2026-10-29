# interop-client

A MoQT test client that drives this repository's `session` library through the
[moq-interop-runner](https://github.com/englishm/moq-interop-runner) test cases,
the inverse of [`cmd/relay`](../relay) (which is tested *by* third-party
clients). It validates our **client-side** code paths against an independent
relay.

It implements the runner's
[client interface](https://github.com/englishm/moq-interop-runner/blob/main/docs/TEST-CLIENT-INTERFACE.md):
TAP version 14 on stdout, exit codes 0 (all passed) / 1 (a failure) / 127
(unknown test).

## Usage

```
interop-client [-r URL] [-t TEST] [-l] [-v] [--tls-disable-verify]
```

| Flag | Env | Description |
|------|-----|-------------|
| `-r`, `--relay` | `RELAY_URL` | Relay URL: `moqt://…` (raw QUIC) or `https://…` (WebTransport) |
| `-t`, `--test` | `TESTCASE` | Run one test (default: all) |
| `-l`, `--list` | — | List test names and exit |
| `-v`, `--verbose` | `VERBOSE` | Diagnostics to stderr (TAP stays on stdout) |
| `--tls-disable-verify` | `TLS_DISABLE_VERIFY` | Skip TLS verification (self-signed dev relays) |

Flags override environment defaults.

## Test cases

All eight are implemented with **no skips** — the `session` library is lower-level
than the consumer APIs that force skips in some other clients:

| Test | What it exercises |
|------|-------------------|
| `setup-only` | Client SETUP handshake, graceful close. |
| `announce-only` | `PUBLISH_NAMESPACE`, verifying `REQUEST_OK`. |
| `publish-namespace-done` | Announce, then withdraw by cancelling the request (§4.1). |
| `subscribe-namespace-lifecycle` | Subscriber `SUBSCRIBE_NAMESPACE`s a namespace unique to the run, `("moq-test", "subscribe-namespace-lifecycle", <128-bit hex run ID>)`; a publisher publishes it, then withdraws it. The subscriber's first two notifications must be `NAMESPACE` and then `NAMESPACE_DONE` for that namespace. A FIN in place of `NAMESPACE_DONE` fails. |
| `subscribe-error` | `SUBSCRIBE` to a non-existent track, expecting an active rejection (`REQUEST_ERROR` or reset; a timeout fails). |
| `rendezvous-timeout` | `SUBSCRIBE` to a track nobody publishes with a 500ms `RENDEZVOUS_TIMEOUT`, expecting `REQUEST_ERROR` with `TIMEOUT` (§9.20.6). `DOES_NOT_EXIST` fails: the relay did not hold the `SUBSCRIBE`. |
| `announce-subscribe` | Publisher announces with `PUBLISH_NAMESPACE` and answers the routed `SUBSCRIBE`; subscriber `SUBSCRIBE`s and gets `SUBSCRIBE_OK`. |
| `subscribe-before-announce` | Subscriber `SUBSCRIBE`s first; publisher announces 500ms later. Draft-22 has the relay answer `REQUEST_ERROR` (no `RENDEZVOUS_TIMEOUT`, §9.20.6); as in the runner, a held `SUBSCRIBE` answered `SUBSCRIBE_OK` passes too. |

The runner's catalog also has `publish-subscribe-media`, a probe of the moqxr
publisher and moqy subscriber with fMP4 media and a catalog. This client does
not expose it (`-t publish-subscribe-media` exits 127).

## Running

From the repository root:

```sh
make interop-client                                   # loopback: our client → our relay (8/8 guard)
make interop-client CLIENT_RELAY_IMAGE=ghcr.io/englishm/moq-interop-runner-moq-relay-ietf-draft-18:latest
make interop-client CLIENT_RELAY_IMAGE=moq-dev-rs-interop:latest   # build the adapter in the runner first
```

For our client against every registered draft-18 relay, use the runner (the
relay is registered there as the `moq-go` client role):

```sh
cd ../moq-interop-runner && make interop-client CLIENT=moq-go
```

Current results are tracked in [`STATUS.md`](../../STATUS.md).
