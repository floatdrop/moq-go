package logctx

import (
	"context"
	"log/slog"
	"testing"
)

func TestGet_returnsDefault_whenNotSet(t *testing.T) {
	logger := Get(context.Background())
	if logger != slog.Default() {
		t.Fatal("expected slog.Default()")
	}
}

func TestWithGet_roundtrip(t *testing.T) {
	custom := slog.New(slog.NewTextHandler(nil, nil))
	ctx := With(context.Background(), custom)
	if got := Get(ctx); got != custom {
		t.Fatal("expected the logger that was set")
	}
}

func TestGet_doesNotAffectParentContext(t *testing.T) {
	parent := context.Background()
	custom := slog.New(slog.NewTextHandler(nil, nil))
	_ = With(parent, custom)
	if Get(parent) != slog.Default() {
		t.Fatal("With must not mutate the parent context")
	}
}

func TestGet_innerContextShadowsOuter(t *testing.T) {
	first := slog.New(slog.NewTextHandler(nil, nil))
	second := slog.New(slog.NewTextHandler(nil, nil))
	ctx := With(context.Background(), first)
	ctx = With(ctx, second)
	if got := Get(ctx); got != second {
		t.Fatal("inner With should shadow the outer one")
	}
}
