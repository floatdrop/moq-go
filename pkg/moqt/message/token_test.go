package message

import (
	"bytes"
	"testing"
)

// ---------------------------------------------------------------------------
// AliasType.String
// ---------------------------------------------------------------------------

func TestAliasTypeString(t *testing.T) {
	tests := []struct {
		at   AliasType
		want string
	}{
		{AliasTypeDelete, "DELETE"},
		{AliasTypeRegister, "REGISTER"},
		{AliasTypeUseAlias, "USE_ALIAS"},
		{AliasTypeUseValue, "USE_VALUE"},
		{AliasType(0xFF), "AliasType(0xFF)"},
	}
	for _, tt := range tests {
		if got := tt.at.String(); got != tt.want {
			t.Errorf("AliasType(%d).String() = %q, want %q", tt.at, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Token round-trip (Bytes / Parse)
// ---------------------------------------------------------------------------

func TestTokenRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		token Token
	}{
		{
			name:  "DELETE",
			token: Token{AliasType: AliasTypeDelete, TokenAlias: 42},
		},
		{
			name: "REGISTER with value",
			token: Token{
				AliasType:  AliasTypeRegister,
				TokenAlias: 7,
				TokenType:  1,
				TokenValue: []byte("bearer-token-payload"),
			},
		},
		{
			name: "REGISTER empty value",
			token: Token{
				AliasType:  AliasTypeRegister,
				TokenAlias: 1,
				TokenType:  0,
				TokenValue: nil,
			},
		},
		{
			name:  "USE_ALIAS",
			token: Token{AliasType: AliasTypeUseAlias, TokenAlias: 99},
		},
		{
			name: "USE_VALUE with value",
			token: Token{
				AliasType:  AliasTypeUseValue,
				TokenType:  2,
				TokenValue: []byte{0xDE, 0xAD, 0xBE, 0xEF},
			},
		},
		{
			name: "USE_VALUE empty value",
			token: Token{
				AliasType:  AliasTypeUseValue,
				TokenType:  0,
				TokenValue: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := tt.token.Bytes()
			var got Token
			if err := got.Parse(raw); err != nil {
				t.Fatalf("Parse() error: %v", err)
			}
			if got.AliasType != tt.token.AliasType {
				t.Errorf("AliasType: got %v, want %v", got.AliasType, tt.token.AliasType)
			}
			if got.TokenAlias != tt.token.TokenAlias {
				t.Errorf("TokenAlias: got %d, want %d", got.TokenAlias, tt.token.TokenAlias)
			}
			if got.TokenType != tt.token.TokenType {
				t.Errorf("TokenType: got %d, want %d", got.TokenType, tt.token.TokenType)
			}
			if !bytes.Equal(got.TokenValue, tt.token.TokenValue) {
				t.Errorf("TokenValue: got %v, want %v", got.TokenValue, tt.token.TokenValue)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Token.Parse error cases
// ---------------------------------------------------------------------------

func TestTokenParseEmpty(t *testing.T) {
	var tok Token
	if err := tok.Parse(nil); err == nil {
		t.Fatal("Parse(nil) expected error, got nil")
	}
}

func TestTokenParseShortBuffer(t *testing.T) {
	// DELETE requires alias after alias type; truncate after alias type byte.
	raw := []byte{0x00} // AliasType=DELETE, but no alias follows
	var tok Token
	if err := tok.Parse(raw); err == nil {
		t.Fatal("Parse() expected error for short DELETE, got nil")
	}
}

func TestTokenParseUnknownAliasType(t *testing.T) {
	// AliasType=0xFF is unknown.
	raw := []byte{0x44} // varint for 0x11 (unknown)
	var tok Token
	if err := tok.Parse(raw); err == nil {
		t.Fatal("Parse() expected error for unknown alias type, got nil")
	}
}

// ---------------------------------------------------------------------------
// TokenSize
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// AuthorizationTokenParam / TokensFromParam
// ---------------------------------------------------------------------------

func TestAuthorizationTokenParamRoundTrip(t *testing.T) {
	tok := Token{
		AliasType:  AliasTypeRegister,
		TokenAlias: 5,
		TokenType:  1,
		TokenValue: []byte("secret"),
	}
	param := AuthorizationTokenParam(tok)
	if param.Type != ParamAuthorizationToken {
		t.Errorf("param.Type = %v, want ParamAuthorizationToken", param.Type)
	}

	ps := Parameters{param}
	tokens, err := TokensFromParam(ps)
	if err != nil {
		t.Fatalf("TokensFromParam() error: %v", err)
	}
	if len(tokens) != 1 {
		t.Fatalf("TokensFromParam() returned %d tokens, want 1", len(tokens))
	}
	got := tokens[0]
	if got.AliasType != tok.AliasType || got.TokenAlias != tok.TokenAlias ||
		got.TokenType != tok.TokenType || !bytes.Equal(got.TokenValue, tok.TokenValue) {
		t.Errorf("round-trip mismatch: got %+v, want %+v", got, tok)
	}
}

func TestTokensFromParamMultiple(t *testing.T) {
	// The spec allows AUTHORIZATION_TOKEN to be repeated.
	t1 := Token{AliasType: AliasTypeUseValue, TokenType: 1, TokenValue: []byte("a")}
	t2 := Token{AliasType: AliasTypeUseValue, TokenType: 2, TokenValue: []byte("b")}
	ps := Parameters{
		AuthorizationTokenParam(t1),
		AuthorizationTokenParam(t2),
	}
	tokens, err := TokensFromParam(ps)
	if err != nil {
		t.Fatalf("TokensFromParam() error: %v", err)
	}
	if len(tokens) != 2 {
		t.Fatalf("got %d tokens, want 2", len(tokens))
	}
}

func TestTokensFromParamAbsent(t *testing.T) {
	ps := Parameters{}
	tokens, err := TokensFromParam(ps)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tokens) != 0 {
		t.Errorf("expected 0 tokens, got %d", len(tokens))
	}
}

func TestTokensFromParamMalformed(t *testing.T) {
	// Inject a raw bytes parameter with invalid Token content.
	ps := Parameters{BytesParam(ParamAuthorizationToken, []byte{})} // empty = no alias type
	_, err := TokensFromParam(ps)
	if err == nil {
		t.Fatal("TokensFromParam() expected error for malformed token, got nil")
	}
}
