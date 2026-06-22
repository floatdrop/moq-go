// Package logctx stores and retrieves a *slog.Logger in a context.Context,
// falling back to slog.Default() when none has been set.
package logctx

import (
	"context"
	"log/slog"
)

type contextKey struct{}

// With returns a new context with the logger attached.
func With(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, logger)
}

// Get retrieves the logger from the context.
// Falls back to slog.Default() if none was set.
func Get(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(contextKey{}).(*slog.Logger); ok {
		return logger
	}
	return slog.Default()
}
