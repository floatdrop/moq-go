package message

import (
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// SetupOption is a MoQT SETUP option type ID (§10.3.1). Distinct from ParamID
// because the two code spaces overlap: option 0x03 is AUTHORIZATION_TOKEN at
// the session level and parameter 0x03 is AUTHORIZATION_TOKEN at the request
// level — different message contexts, different parsing rules. The underlying
// wire field (wire.KVPair.Type) stays uint64 because KVPair is wire-generic.
type SetupOption uint64

const (
	SetupOptionPath               SetupOption = 0x01
	SetupOptionAuthorizationToken SetupOption = 0x03
	SetupOptionMaxAuthTokenCache  SetupOption = 0x04
	SetupOptionAuthority          SetupOption = 0x05
	SetupOptionMOQTImplementation SetupOption = 0x07
)

// Setup carries the SETUP message payload (§10.3). Setup Options span the
// remainder of the message payload as a delta-encoded sequence of KVPairs.
type Setup struct {
	Options []wire.KVPair
}

// PathOption builds a PATH setup option (§10.3.1.2). Client-only, native-QUIC
// only: a PATH option received by a server, on a WebTransport session, or with
// an unsupported path triggers an INVALID_PATH session close. pathAndQuery is
// the path-abempty portion of the moqt URI, optionally followed by "?" and
// the query.
func PathOption(pathAndQuery string) wire.KVPair {
	return wire.KVPair{Type: uint64(SetupOptionPath), ByteVal: []byte(pathAndQuery)}
}

// AuthorityOption builds an AUTHORITY setup option (§10.3.1.1). Client-only,
// native-QUIC only: a server-sent or WebTransport-sent AUTHORITY triggers
// INVALID_AUTHORITY. authority is the authority portion of the moqt URI.
func AuthorityOption(authority string) wire.KVPair {
	return wire.KVPair{Type: uint64(SetupOptionAuthority), ByteVal: []byte(authority)}
}

// MOQTImplementationOption builds a MOQT_IMPLEMENTATION setup option
// (§10.3.1.5). Optional; intended for debugging and interop tracking. nameAndVersion
// SHOULD be the implementation name plus version (e.g. "mediamesh/0.1.0").
func MOQTImplementationOption(nameAndVersion string) wire.KVPair {
	return wire.KVPair{Type: uint64(SetupOptionMOQTImplementation), ByteVal: []byte(nameAndVersion)}
}

// MaxAuthTokenCacheSizeOption builds a MAX_AUTH_TOKEN_CACHE_SIZE option
// (§10.3.1.3). maxBytes is the peer-allowed total size in bytes of registered
// authorization tokens. The default if omitted is 0, which prohibits the use
// of token Aliases.
func MaxAuthTokenCacheSizeOption(maxBytes uint64) wire.KVPair {
	return wire.KVPair{Type: uint64(SetupOptionMaxAuthTokenCache), IntVal: maxBytes}
}

func (m *Setup) Type() Type { return TypeSetup }

func (m *Setup) Append(w *wire.Writer) {
	w.KVPairs(m.Options)
}

func (m *Setup) Parse(r *wire.Reader) error {
	s := r.Scanner()
	s.KVPairsRemaining(&m.Options)
	return s.Err()
}
