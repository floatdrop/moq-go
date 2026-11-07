//go:build !linux

package relaynet

import "testing"

// Off Linux, a listener keeps its one socket handle whatever it is asked for:
// parallel sends on one socket measured slower on macOS. The plain quic-go
// listener needs no cleanup hook; the parallel one returns one.
func TestUDPSendWritersStayOneOffLinux(t *testing.T) {
	t.Parallel()
	tlsCfg, err := TLSConfig("", "", DualALPNs)
	if err != nil {
		t.Fatal(err)
	}
	l, cleanup, err := listenQUIC("127.0.0.1:0", tlsCfg, defaultQUICConfig(), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if cleanup != nil {
		t.Error("asked for 4 handles off Linux, the listener built them")
	}
}
