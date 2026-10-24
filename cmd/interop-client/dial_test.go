package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session/wtconn"
	"github.com/floatdrop/moq-go/pkg/relay/relaynet"
)

// serveWebTransport serves a WebTransport endpoint at https://127.0.0.1:port/,
// offering protocols for negotiation (§6.2.1), and reports each session's
// negotiated protocol on the returned channel.
func serveWebTransport(t *testing.T, protocols []string) (rawURL string, negotiated <-chan string) {
	t.Helper()
	cert, err := relaynet.SelfSignedCert()
	if err != nil {
		t.Fatalf("SelfSignedCert: %v", err)
	}
	mux := http.NewServeMux()
	h3 := &http3.Server{
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{http3.NextProtoH3}},
		Handler:   mux,
	}
	webtransport.ConfigureHTTP3Server(h3)
	wts := &webtransport.Server{H3: h3, ApplicationProtocols: protocols}
	ch := make(chan string, 1)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		sess, err := wts.Upgrade(w, r)
		if err != nil {
			t.Errorf("Upgrade: %v", err)
			return
		}
		ch <- sess.SessionState().ApplicationProtocol
		<-sess.Context().Done()
	})
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	go func() { _ = wts.Serve(udp) }()
	t.Cleanup(func() { _ = wts.Close() })
	return "https://" + udp.LocalAddr().String() + "/", ch
}

// TestWebTransportOffersTheMOQTProtocol: the interop client's WebTransport
// CONNECT offers "moqt-22" in WT-Available-Protocols, as §6.2.1 requires ("The
// client includes MOQT protocol identifiers"), so the draft version is
// negotiated. Without it imquic refused the session ("No WebTransport protocol
// offered") and aiomoqt-relay closed it after SETUP.
func TestWebTransportOffersTheMOQTProtocol(t *testing.T) {
	rawURL, negotiated := serveWebTransport(t, []string{"moqt-22"})
	h := &harness{relayURL: rawURL, insecure: true}
	conn, err := wtconn.Dial(t.Context(), h.webTransport(), rawURL)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.CloseWithError(0, "") })
	select {
	case got := <-negotiated:
		if got != "moqt-22" {
			t.Fatalf("negotiated WebTransport protocol %q, want %q", got, "moqt-22")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the server never saw the session")
	}
}

// TestWebTransportReachesServersWithoutResetStreamAt: the interop client
// reaches a WebTransport server that did not enable RESET_STREAM_AT, which
// webtransport-go's client refuses by default (draft-ietf-webtrans-http3
// requires it). moq-dev-rs, moxygen, moqtail and stitcher-moq did not enable
// it, and the client failed every test against them with "server didn't
// enable QUIC stream reset partial delivery".
func TestWebTransportReachesServersWithoutResetStreamAt(t *testing.T) {
	cert, err := relaynet.SelfSignedCert()
	if err != nil {
		t.Fatalf("SelfSignedCert: %v", err)
	}
	// webtransport.Server always enables RESET_STREAM_AT, so this is HTTP/3
	// with the WebTransport settings on a QUIC listener without it.
	ln, err := quic.ListenAddr("127.0.0.1:0",
		&tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{http3.NextProtoH3}},
		&quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatalf("ListenAddr: %v", err)
	}
	h3 := &http3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})}
	webtransport.ConfigureHTTP3Server(h3)
	go func() { _ = h3.ServeListener(ln) }()
	t.Cleanup(func() { _ = h3.Close() })

	rawURL := "https://" + ln.Addr().String() + "/"
	h := &harness{relayURL: rawURL, insecure: true}
	conn, err := wtconn.Dial(t.Context(), h.webTransport(), rawURL)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = conn.CloseWithError(0, "")
}
