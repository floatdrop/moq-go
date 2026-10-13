package relaynet_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

// TestDialWebTransport_FlowControl: the dialing side's SETTINGS grant the peer
// streams and data too. The upstream of a cross-relay WebTransport leg opens a stream
// towards the dialer for every subgroup it forwards, which a peer enforcing the
// draft's zero default could not do if the dialer advertised nothing.
func TestDialWebTransport_FlowControl(t *testing.T) {
	t.Parallel()
	tlsCfg, err := relaynet.TLSConfig("", "", relaynet.WebTransportALPNs)
	if err != nil {
		t.Fatal(err)
	}
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan map[uint64]uint64, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/moq", func(w http.ResponseWriter, _ *http.Request) {
		s := w.(http3.Settingser)
		<-s.ReceivedSettings()
		got <- s.Settings().Other
		w.WriteHeader(http.StatusNotFound)
	})
	h3 := &http3.Server{
		TLSConfig:  tlsCfg,
		Handler:    mux,
		QUICConfig: &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true},
	}
	webtransport.ConfigureHTTP3Server(h3)
	wts := &webtransport.Server{H3: h3}
	go func() { _ = wts.Serve(udp) }()
	t.Cleanup(func() { _ = wts.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// The server refuses the CONNECT once it has the SETTINGS; the dial error
	// is expected and only the SETTINGS matter.
	url := fmt.Sprintf("https://%s/moq", udp.LocalAddr())
	_, _ = relaynet.DialWebTransport(ctx, url, relaynet.InsecureClientTLSConfig(relaynet.WebTransportALPNs))

	var settings map[uint64]uint64
	select {
	case settings = <-got:
	case <-ctx.Done():
		t.Fatal("the dialer's CONNECT never reached the server")
	}
	for name, id := range flowControlSettings {
		if settings[id] == 0 {
			t.Errorf("dialer's SETTINGS grant the peer no initial %s (%+v)", name, settings)
		}
	}
}
