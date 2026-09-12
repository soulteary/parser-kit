package parserkit

import (
	"math"
	"testing"
	"time"
)

// The retry policy handed to http-kit must carry a positive MaxRetryDelay.
//
// http-kit's CalculateRetryDelay ends with
//
//	if delay > r.MaxRetryDelay || delay < 0 { delay = r.MaxRetryDelay }
//
// so the ceiling is applied unconditionally: with MaxRetryDelay left at zero,
// every computed delay is greater than it and gets clamped to zero. The retry
// loop then hammers a failing remote source as fast as the network allows,
// while RetryDelay and BackoffMultiplier look configured and do nothing.
func TestRetryOptionsKeepTheBackoff(t *testing.T) {
	l, err := NewLoader[map[string]string](DefaultLoadOptions())
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	opts := l.(*loader[map[string]string]).retryOptions()

	if opts.MaxRetryDelay <= 0 {
		t.Fatalf("MaxRetryDelay = %v, want > 0: a zero ceiling clamps every delay to zero", opts.MaxRetryDelay)
	}

	// The delay that actually reaches time.After must be positive and must grow.
	first := opts.CalculateRetryDelay(0)
	second := opts.CalculateRetryDelay(1)
	if first <= 0 {
		t.Fatalf("CalculateRetryDelay(0) = %v, want > 0", first)
	}
	if second <= first {
		t.Errorf("CalculateRetryDelay(1) = %v, want > CalculateRetryDelay(0) = %v", second, first)
	}
	if second > opts.MaxRetryDelay {
		t.Errorf("CalculateRetryDelay(1) = %v exceeds the ceiling %v", second, opts.MaxRetryDelay)
	}
}

// A caller that builds LoadOptions as a struct literal -- the documented way to
// override a field or two -- must not lose the backoff by omitting the new one.
func TestZeroMaxRetryDelayFallsBackToDefault(t *testing.T) {
	l, err := NewLoader[map[string]string](&LoadOptions{
		MaxRetries: 5,
		RetryDelay: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	opts := l.(*loader[map[string]string]).retryOptions()
	if want := DefaultLoadOptions().MaxRetryDelay; opts.MaxRetryDelay != want {
		t.Fatalf("MaxRetryDelay = %v, want the default %v", opts.MaxRetryDelay, want)
	}
	// The exact curve is http-kit's business and differs between its versions;
	// what must hold here is that the configured delay survives the ceiling.
	got := opts.CalculateRetryDelay(0)
	if got <= 0 {
		t.Errorf("CalculateRetryDelay(0) = %v, want > 0", got)
	}
	if got > opts.MaxRetryDelay {
		t.Errorf("CalculateRetryDelay(0) = %v exceeds the ceiling %v", got, opts.MaxRetryDelay)
	}
}

// An explicit ceiling is honoured rather than overwritten by the default.
func TestExplicitMaxRetryDelayIsKept(t *testing.T) {
	l, err := NewLoader[map[string]string](&LoadOptions{
		RetryDelay:    1 * time.Second,
		MaxRetryDelay: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	opts := l.(*loader[map[string]string]).retryOptions()
	if opts.MaxRetryDelay != 2*time.Second {
		t.Fatalf("MaxRetryDelay = %v, want 2s", opts.MaxRetryDelay)
	}
	// 1s * 2^3 = 8s, clamped to the 2s ceiling.
	if got := opts.CalculateRetryDelay(3); got != 2*time.Second {
		t.Errorf("CalculateRetryDelay(3) = %v, want the 2s ceiling", got)
	}
}

// Normalisation must not write back into the caller's struct, matching the
// guarantee NewLoader already makes for the other fields.
func TestMaxRetryDelayNormalisationDoesNotMutateCaller(t *testing.T) {
	opts := &LoadOptions{MaxRetries: 1}
	if _, err := NewLoader[map[string]string](opts); err != nil {
		t.Fatalf("NewLoader: %v", err)
	}
	if opts.MaxRetryDelay != 0 {
		t.Errorf("caller's MaxRetryDelay = %v, want it left at 0", opts.MaxRetryDelay)
	}
}

// MaxFileSize's "no limit" spelling still works alongside the new field.
func TestUnlimitedFileSizeStillResolves(t *testing.T) {
	if got := readLimit(math.MaxInt64); got != math.MaxInt64 {
		t.Errorf("readLimit(MaxInt64) = %d, want MaxInt64", got)
	}
}
