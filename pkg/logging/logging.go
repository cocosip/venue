// Package logging provides instance-scoped structured logging for Venue.
package logging

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
)

// Config configures a Venue logging runtime. The caller owns the handler and
// any writer or resource used by it.
//
// Handler is runtime-only: it is never bound from JSON, YAML, or Viper, so all
// binding tags are "-".
type Config struct {
	Handler slog.Handler `json:"-" yaml:"-" mapstructure:"-"`
}

// Record is one structured Venue log event.
type Record struct {
	Level     slog.Level
	Component string
	Event     string
	Message   string
	Attrs     []slog.Attr
}

type runtimeState struct {
	logger *slog.Logger
}

// Runtime is a private logger for one Venue instance. It never reads or
// modifies slog.Default.
type Runtime struct {
	state atomic.Pointer[runtimeState]
}

// New creates an enabled logging runtime.
func New(config Config) (*Runtime, error) {
	if config.Handler == nil {
		return nil, errors.New("logging handler cannot be nil")
	}

	runtime := &Runtime{}
	runtime.state.Store(&runtimeState{logger: slog.New(config.Handler)})
	return runtime, nil
}

// Disabled creates a silent logging runtime.
func Disabled() *Runtime {
	return &Runtime{}
}

// Disable atomically disables this runtime. It does not close the configured
// handler or its underlying writer.
func (r *Runtime) Disable() {
	if r != nil {
		r.state.Store(nil)
	}
}

// Enabled reports whether this runtime accepts records at level.
func (r *Runtime) Enabled(ctx context.Context, level slog.Level) (enabled bool) {
	defer func() {
		if recover() != nil {
			enabled = false
		}
	}()
	if r == nil {
		return false
	}
	current := r.state.Load()
	return current != nil && current.logger.Enabled(normalizeContext(ctx), level)
}

// Emit writes a structured record through this runtime. Handler panics are
// contained so logging cannot break storage operations.
func (r *Runtime) Emit(ctx context.Context, record Record) {
	defer func() {
		_ = recover()
	}()
	if r == nil {
		return
	}
	current := r.state.Load()
	ctx = normalizeContext(ctx)
	if current == nil || !current.logger.Enabled(ctx, record.Level) {
		return
	}

	attrs := []slog.Attr{
		slog.String("component", record.Component),
		slog.String("event", record.Event),
	}
	attrs = append(attrs, record.Attrs...)
	current.logger.LogAttrs(ctx, record.Level, record.Message, attrs...)
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
