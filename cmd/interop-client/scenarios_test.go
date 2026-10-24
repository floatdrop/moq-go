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
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

// startRelay runs this repository's relay over raw QUIC on loopback, gated
// by auth (nil: allow everything), and returns its moqt:// URL.
func startRelay(t *testing.T, auth relay.Authorizer) string {
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

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r := relay.New(quicconn.NewListener(ql), relay.Config{Authorizer: auth, GoawayTimeout: 100 * time.Millisecond})
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
	relayURL := startRelay(t, noPushNoDiscovery{})
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
	relayURL := startRelay(t, nil)
	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			if err := runScenario(t, relayURL, s.name); err != nil {
				t.Fatal(err)
			}
		})
	}
}
