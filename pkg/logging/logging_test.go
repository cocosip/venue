package logging

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

func TestDisabledRuntimeIsSilent(t *testing.T) {
	runtime := Disabled()

	if runtime.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("Enabled() = true for disabled runtime")
	}
	runtime.Emit(context.Background(), Record{
		Level: slog.LevelError, Component: "test", Event: "disabled", Message: "not emitted",
	})
}

// TestNewRejectsNilHandler pins both halves of the contract: a nil handler is
// rejected, and the rejection is classifiable through core.ErrInvalidArgument so
// a caller of NewVenue can treat it like every other public configuration error
// instead of matching on message text.
func TestNewRejectsNilHandler(t *testing.T) {
	runtime, err := New(Config{})
	if err == nil {
		t.Fatal("New() error = nil, want nil-handler error")
	}
	if !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("New() error = %v, want it to wrap core.ErrInvalidArgument", err)
	}
	if runtime != nil {
		t.Fatalf("New() runtime = %v, want nil alongside the error", runtime)
	}
}

func TestRuntimeDoesNotUseSlogDefault(t *testing.T) {
	defaultHandler := &recordingHandler{}
	configuredHandler := &recordingHandler{}
	previousDefault := slog.Default()
	slog.SetDefault(slog.New(defaultHandler))
	t.Cleanup(func() { slog.SetDefault(previousDefault) })

	runtime, err := New(Config{Handler: configuredHandler})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	runtime.Emit(context.Background(), Record{
		Level: slog.LevelInfo, Component: "venue.test", Event: "configured", Message: "emitted",
		Attrs: []slog.Attr{slog.String("key", "value")},
	})

	records := configuredHandler.snapshot()
	if len(records) != 1 {
		t.Fatalf("configured handler records = %d, want 1", len(records))
	}
	if got := len(defaultHandler.snapshot()); got != 0 {
		t.Fatalf("default handler records = %d, want 0", got)
	}
	attrs := recordAttrs(records[0])
	if attrs["component"] != "venue.test" || attrs["event"] != "configured" || attrs["key"] != "value" {
		t.Fatalf("record attrs = %#v", attrs)
	}
}

func TestDisableStopsEmission(t *testing.T) {
	handler := &recordingHandler{}
	runtime, err := New(Config{Handler: handler})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	runtime.Disable()
	runtime.Emit(context.Background(), Record{
		Level: slog.LevelError, Component: "venue.test", Event: "disabled", Message: "not emitted",
	})

	if runtime.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("Enabled() = true after Disable()")
	}
	if got := len(handler.snapshot()); got != 0 {
		t.Fatalf("records after Disable() = %d, want 0", got)
	}
}

func TestHandlerPanicDoesNotEscapeRuntime(t *testing.T) {
	t.Run("Enabled", func(t *testing.T) {
		runtime, err := New(Config{Handler: panicHandler{panicEnabled: true}})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if runtime.Enabled(context.TODO(), slog.LevelInfo) {
			t.Fatal("Enabled() = true after handler panic")
		}
	})

	t.Run("Handle", func(t *testing.T) {
		runtime, err := New(Config{Handler: panicHandler{panicHandle: true}})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		runtime.Emit(context.TODO(), Record{
			Level: slog.LevelInfo, Component: "venue.test", Event: "panic", Message: "not emitted",
		})
	})
}

func TestConcurrentEmitAndDisable(t *testing.T) {
	runtime, err := New(Config{Handler: &recordingHandler{}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const goroutines = 8
	const iterations = 100
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				runtime.Emit(context.Background(), Record{
					Level: slog.LevelDebug, Component: "venue.test", Event: "concurrent", Message: "concurrent",
				})
				runtime.Disable()
			}
		}()
	}
	wg.Wait()
}

type panicHandler struct {
	panicEnabled bool
	panicHandle  bool
}

func (h panicHandler) Enabled(context.Context, slog.Level) bool {
	if h.panicEnabled {
		panic("Enabled")
	}
	return true
}

func (h panicHandler) Handle(context.Context, slog.Record) error {
	if h.panicHandle {
		panic("Handle")
	}
	return nil
}

func (h panicHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h panicHandler) WithGroup(string) slog.Handler { return h }

type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record.Clone())
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recordingHandler) WithGroup(string) slog.Handler { return h }

func (h *recordingHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}

func recordAttrs(record slog.Record) map[string]any {
	attrs := make(map[string]any)
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	return attrs
}
