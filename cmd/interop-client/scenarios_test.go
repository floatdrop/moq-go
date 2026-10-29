package main

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

// listenQUIC listens for raw QUIC on loopback with a self-signed certificate,
// offering the draft the client dials.
func listenQUIC(t *testing.T) *quic.Listener {
	t.Helper()
	cert, err := relaynet.SelfSignedCert()
	if err != nil {
		t.Fatalf("SelfSignedCert: %v", err)
	}
	ql, err := quic.ListenAddr("127.0.0.1:0",
		&tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: moqtProtocols},
		&quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true})
	if err != nil {
		t.Fatalf("ListenAddr: %v", err)
	}
	t.Cleanup(func() { _ = ql.Close() })
	return ql
}

// startRelay runs this repository's relay, configured by cfg, over raw QUIC
// on loopback and returns its moqt:// URL.
func startRelay(t *testing.T, cfg relay.Config) string {
	t.Helper()
	ql := listenQUIC(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg.GoawayTimeout = 100 * time.Millisecond
	r := relay.New(quicconn.NewListener(ql), cfg)
	go func() { _ = r.Start(ctx) }()
	t.Cleanup(func() {
		sctx, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = r.Stop(sctx)
	})
	return "moqt://" + ql.Addr().String()
}

// noPushNoDiscovery is a relay that, like moq-dev-rs and stitcher-moq, refuses
// PUBLISH and SUBSCRIBE_NAMESPACE as NOT_SUPPORTED. Neither is part of the
// runner's procedure for the scenarios that publish and subscribe a track.
type noPushNoDiscovery struct{ relay.AllowAllAuthorizer }

func (noPushNoDiscovery) AuthorizePublish(context.Context, *session.Session, *message.Publish) error {
	return relay.Deny(moqt.RequestNotSupported, "PUBLISH is not supported")
}

func (noPushNoDiscovery) AuthorizeSubscribeNamespace(
	context.Context, *session.Session, *message.SubscribeNamespace,
) error {
	return relay.Deny(moqt.RequestNotSupported, "SUBSCRIBE_NAMESPACE is not supported")
}

// runScenario runs the named scenario against relayURL with its own budget.
func runScenario(t *testing.T, relayURL, name string) error {
	t.Helper()
	s, ok := findScenario(name)
	if !ok {
		t.Fatalf("no scenario %q", name)
	}
	ctx, cancel := context.WithTimeout(t.Context(), perTestTimeout(name))
	defer cancel()
	return s.run(ctx, &harness{relayURL: relayURL, insecure: true})
}

// TestScenariosFollowTheRunnersProcedure: announce-subscribe and
// subscribe-before-announce use only the messages the runner's TEST-CASES.md
// procedure names, PUBLISH_NAMESPACE and SUBSCRIBE, so a relay that supports
// neither PUBLISH nor SUBSCRIBE_NAMESPACE still passes them. The publisher
// answers the SUBSCRIBE the relay routes to it. Against moq-dev-rs and
// stitcher-moq, the client's PUBLISH was refused ("PUBLISH is not supported"),
// and against imquic it waited for a NAMESPACE that never came.
func TestScenariosFollowTheRunnersProcedure(t *testing.T) {
	relayURL := startRelay(t, relay.Config{Authorizer: noPushNoDiscovery{}})
	for _, name := range []string{"announce-subscribe", "subscribe-before-announce"} {
		t.Run(name, func(t *testing.T) {
			if err := runScenario(t, relayURL, name); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestScenariosPassAgainstThisRelay runs every scenario against this
// repository's relay, as the interop runner's moq-go → moq-go pairing does.
func TestScenariosPassAgainstThisRelay(t *testing.T) {
	relayURL := startRelay(t, relay.Config{})
	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			if err := runScenario(t, relayURL, s.name); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestRendezvousTimeoutNeedsTheRelayToHold: a relay that ignores
// RENDEZVOUS_TIMEOUT and refuses the SUBSCRIBE at once with DOES_NOT_EXIST
// fails rendezvous-timeout, which would otherwise pass any rejection, as
// subscribe-error does. This relay holds no SUBSCRIBE.
func TestRendezvousTimeoutNeedsTheRelayToHold(t *testing.T) {
	relayURL := startRelay(t, relay.Config{MaxRendezvousTimeout: -1})
	if err := runScenario(t, relayURL, "rendezvous-timeout"); err == nil {
		t.Fatal("rendezvous-timeout passed against a relay that answered DOES_NOT_EXIST at once")
	}
}

// scriptedNamespaceRelay is a relay that accepts every PUBLISH_NAMESPACE and
// SUBSCRIBE_NAMESPACE, and answers each SUBSCRIBE_NAMESPACE with script and
// then a FIN, whatever is published. It returns its moqt:// URL.
func scriptedNamespaceRelay(t *testing.T, script ...message.Message) string {
	t.Helper()
	l := quicconn.NewListener(listenQUIC(t))
	go func() {
		for {
			conn, err := l.Accept(t.Context())
			if err != nil {
				return
			}
			go func() {
				sess, err := session.Server(t.Context(), conn)
				if err != nil {
					return
				}
				for {
					r, err := sess.AcceptRequest(t.Context())
					if err != nil {
						return
					}
					switch r.First.(type) {
					case *message.PublishNamespace:
						_, _ = r.AcceptPublishNamespace()
					case *message.SubscribeNamespace:
						sub, err := r.AcceptSubscribeNamespace()
						if err != nil {
							return
						}
						for _, m := range script {
							_ = message.Marshal(sub, m)
						}
						_ = sub.Close()
					default:
						_ = r.RejectError(moqt.RequestNotSupported, "scripted relay")
					}
				}
			}()
		}
	}()
	return "moqt://" + l.Addr().String()
}

// TestSubscribeNamespaceLifecycleNeedsNamespaceThenDone: the subscriber's
// first two notifications must be NAMESPACE and then NAMESPACE_DONE, both for
// the namespace the publisher published. The subscribed prefix is that whole
// namespace, so their suffix is empty (§9.16, §9.17).
func TestSubscribeNamespaceLifecycleNeedsNamespaceThenDone(t *testing.T) {
	for _, tc := range []struct {
		name   string
		script []message.Message
	}{
		// §4.2.2 has a subscriber treat a FIN as NAMESPACE_DONE, but the
		// runner's criteria name the message itself.
		{"FIN for NAMESPACE_DONE", []message.Message{&message.Namespace{}}},
		{"NAMESPACE twice", []message.Message{
			&message.Namespace{}, &message.Namespace{}, &message.NamespaceDone{},
		}},
		{"a namespace nobody published", []message.Message{
			&message.Namespace{TrackNamespaceSuffix: wire.Namespace("other")},
			&message.Namespace{}, &message.NamespaceDone{},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			relayURL := scriptedNamespaceRelay(t, tc.script...)
			if err := runScenario(t, relayURL, "subscribe-namespace-lifecycle"); err == nil {
				t.Fatal("subscribe-namespace-lifecycle passed")
			}
		})
	}

	// The control case: the same relay, with the sequence right.
	relayURL := scriptedNamespaceRelay(t, &message.Namespace{}, &message.NamespaceDone{})
	if err := runScenario(t, relayURL, "subscribe-namespace-lifecycle"); err != nil {
		t.Fatalf("NAMESPACE then NAMESPACE_DONE: %v", err)
	}
}
