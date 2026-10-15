package relaynet_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

// TestTransportStatsBothTransports: a session reports the figures of the QUIC
// connection under it on either transport, on both ends: the listener's
// WebTransport sessions know their connection through ConnContext, the
// dialer's through the connection it dialed.
func TestTransportStatsBothTransports(t *testing.T) {
	t.Parallel()
	const wtPath = "/moq"
	tlsCfg, err := relaynet.TLSConfig("", "", relaynet.DualALPNs)
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}
	l, err := relaynet.Listen("127.0.0.1:0", wtPath, tlsCfg, nil)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { l.Close() })

	servers := make(chan *session.Session, 2)
	go func() {
		for {
			conn, err := l.Accept(context.Background())
			if err != nil {
				return
			}
			go func() {
				if s, err := session.Server(context.Background(), conn); err == nil {
					servers <- s
				}
			}()
		}
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	rawConn, err := relaynet.DialQUIC(ctx, l.Addr().String(), relaynet.InsecureClientTLSConfig(relaynet.MOQTQUICALPNs))
	if err != nil {
		t.Fatalf("DialQUIC: %v", err)
	}
	wtConn, err := relaynet.DialWebTransport(ctx, "https://"+l.Addr().String()+wtPath,
		relaynet.InsecureClientTLSConfig(relaynet.WebTransportALPNs))
	if err != nil {
		t.Fatalf("DialWebTransport: %v", err)
	}
	sessions := map[string]*session.Session{}
	for name, conn := range map[string]session.Conn{"raw QUIC client": rawConn, "WebTransport client": wtConn} {
		s, err := session.Client(ctx, conn)
		if err != nil {
			t.Fatalf("%s SETUP: %v", name, err)
		}
		t.Cleanup(func() { s.Close(0, "done") })
		sessions[name] = s
	}
	for i := range 2 {
		select {
		case s := <-servers:
			t.Cleanup(func() { s.Close(0, "done") })
			sessions[[]string{"server A", "server B"}[i]] = s
		case <-ctx.Done():
			t.Fatalf("listener served %d of 2 sessions", i)
		}
	}

	for name, s := range sessions {
		st, ok := s.TransportStats()
		if !ok {
			t.Errorf("%s: TransportStats not reported", name)
			continue
		}
		// SETUP went both ways, and was acknowledged: there are packets each way
		// and an RTT estimate.
		if st.PacketsSent == 0 || st.PacketsReceived == 0 || st.BytesSent == 0 || st.SmoothedRTT <= 0 {
			t.Errorf("%s: TransportStats = %+v, want packets and bytes both ways and an RTT", name, st)
		}
	}
}
