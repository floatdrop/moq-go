package message

import (
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// AliasType identifies the serialization and processing behavior of a Token
// per §10.2.2.
type AliasType uint64

const (
	// AliasTypeDelete (0x0): Alias only. Retire the alias and its associated
	// token from the cache.
	AliasTypeDelete AliasType = 0x0

	// AliasTypeRegister (0x1): Alias + Type + Value. Register the alias in
	// the token cache for the duration of the session (or until deleted).
	AliasTypeRegister AliasType = 0x1

	// AliasTypeUseAlias (0x2): Alias only. Resolve to the (Type, Value)
	// previously registered under this alias.
	AliasTypeUseAlias AliasType = 0x2

	// AliasTypeUseValue (0x3): Type + Value only. Use the token directly;
	// no alias is stored.
	AliasTypeUseValue AliasType = 0x3
)

// String returns a human-readable name for the alias type.
func (a AliasType) String() string {
	switch a {
	case AliasTypeDelete:
		return "DELETE"
	case AliasTypeRegister:
		return "REGISTER"
	case AliasTypeUseAlias:
		return "USE_ALIAS"
	case AliasTypeUseValue:
		return "USE_VALUE"
	}
	return fmt.Sprintf("AliasType(0x%X)", uint64(a))
}

// Token is the Token structure from §10.2.2.
//
// Wire format (within the outer KindBytes length-prefixed parameter value):
//
//	Token {
//	  Alias Type (vi64),
//	  [Token Alias (vi64),]   -- DELETE, REGISTER, USE_ALIAS
//	  [Token Type (vi64),]    -- REGISTER, USE_VALUE
//	  [Token Value (..)]      -- REGISTER, USE_VALUE; raw bytes to end of value
//	}
//
// TokenValue has no inner length prefix; it occupies the remainder of the
// outer KindBytes parameter value.
type Token struct {
	AliasType  AliasType
	TokenAlias uint64 // present for DELETE, REGISTER, USE_ALIAS
	TokenType  uint64 // present for REGISTER, USE_VALUE
	TokenValue []byte // present for REGISTER, USE_VALUE
}

// Append serialises t into w. The caller is responsible for the outer
// KindBytes length prefix (handled by params.go via VarintBytes).
func (t *Token) Append(w *wire.Writer) {
	w.Varint(uint64(t.AliasType))
	switch t.AliasType {
	case AliasTypeDelete:
		w.Varint(t.TokenAlias)
	case AliasTypeRegister:
		w.Varint(t.TokenAlias)
		w.Varint(t.TokenType)
		w.FixedBytes(t.TokenValue)
	case AliasTypeUseAlias:
		w.Varint(t.TokenAlias)
	case AliasTypeUseValue:
		w.Varint(t.TokenType)
		w.FixedBytes(t.TokenValue)
	}
}

// Bytes returns the serialised Token as a byte slice, suitable for use as the
// value of a KindBytes AUTHORIZATION_TOKEN parameter.
func (t *Token) Bytes() []byte {
	var w wire.Writer
	t.Append(&w)
	return w.Bytes()
}

// Parse deserialises a Token from raw — the raw bytes of a KindBytes parameter
// value. Returns an error (caller should map to KEY_VALUE_FORMATTING_ERROR) if
// the bytes are malformed.
func (t *Token) Parse(raw []byte) error {
	r := wire.NewReader(raw)

	at, err := r.Varint()
	if err != nil {
		return fmt.Errorf("moqt/message: token alias type: %w", err)
	}
	t.AliasType = AliasType(at)

	switch t.AliasType {
	case AliasTypeDelete:
		alias, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: token alias (DELETE): %w", err)
		}
		t.TokenAlias = alias

	case AliasTypeRegister:
		alias, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: token alias (REGISTER): %w", err)
		}
		tokenType, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: token type (REGISTER): %w", err)
		}
		// TokenValue occupies the remainder of the parameter value.
		t.TokenAlias = alias
		t.TokenType = tokenType
		t.TokenValue = r.RemainingBytes()

	case AliasTypeUseAlias:
		alias, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: token alias (USE_ALIAS): %w", err)
		}
		t.TokenAlias = alias

	case AliasTypeUseValue:
		tokenType, err := r.Varint()
		if err != nil {
			return fmt.Errorf("moqt/message: token type (USE_VALUE): %w", err)
		}
		t.TokenType = tokenType
		t.TokenValue = r.RemainingBytes()

	default:
		return fmt.Errorf("moqt/message: unknown token alias type 0x%X", at)
	}

	return nil
}

// AuthorizationTokenParam builds a typed AUTHORIZATION_TOKEN parameter
// (§10.2.2) from a Token. The Token is serialised to bytes and stored as a
// KindBytes parameter.
func AuthorizationTokenParam(t Token) Parameter {
	return BytesParam(ParamAuthorizationToken, t.Bytes())
}

// TokensFromParam extracts and parses all AUTHORIZATION_TOKEN parameters from
// ps. The spec allows the parameter to be repeated within a message (§10.2.2:
// "The AUTHORIZATION TOKEN parameter MAY be repeated within a message as long
// as the combination of Token Type and Token Value are unique after resolving
// any aliases"). Returns an error if any Token is malformed.
func TokensFromParam(ps Parameters) ([]Token, error) {
	var tokens []Token
	for _, p := range ps {
		if p.Type != ParamAuthorizationToken {
			continue
		}
		var t Token
		if err := t.Parse(p.Bytes); err != nil {
			return nil, fmt.Errorf("moqt/message: AUTHORIZATION_TOKEN: %w", err)
		}
		tokens = append(tokens, t)
	}
	return tokens, nil
}
