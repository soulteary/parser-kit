package remotesource_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	httpkit "github.com/soulteary/http-kit/v2"

	parserkit "github.com/soulteary/parser-kit/v2"
	"github.com/soulteary/parser-kit/v2/remotesource"
)

// This package used to call otel.GetTextMapPropagator() on every fetch, so a
// service that configured a global OpenTelemetry propagator got trace headers
// without asking -- and every service importing this package linked
// OpenTelemetry whether it traced or not. Propagation is opt-in now. These
// tests pin both halves: nothing by default, and the whole of it when asked.

type contextKey struct{}

func TestNoPropagationByDefault(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	fetcher, err := remotesource.New(srv.URL, remotesource.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := fetcher.Fetch(context.Background(), parserkit.DefaultMaxBytes); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if v := got.Get("X-Trace-Id"); v != "" {
		t.Errorf("X-Trace-Id = %q on a source that was not asked to propagate", v)
	}
}

func TestWithPropagatorInjectsOnEveryRequest(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	// Stands in for otelprop.Global(), which is the same shape without
	// dragging OpenTelemetry into this module's tests.
	propagator := httpkit.PropagatorFunc(func(ctx context.Context, h http.Header) {
		if id, ok := ctx.Value(contextKey{}).(string); ok {
			h.Set("X-Trace-Id", id)
		}
	})

	fetcher, err := remotesource.New(srv.URL,
		remotesource.WithTimeout(5*time.Second),
		remotesource.WithPropagator(propagator),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.WithValue(context.Background(), contextKey{}, "trace-abc")
	if _, err := fetcher.Fetch(ctx, parserkit.DefaultMaxBytes); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if v := got.Get("X-Trace-Id"); v != "trace-abc" {
		t.Errorf("X-Trace-Id = %q, want trace-abc", v)
	}
}

// The injection happens inside http-kit's Do, which runs on every retry
// attempt -- so a request that is replayed still carries the headers. The old
// call site injected once, before the retry loop.
func TestPropagationSurvivesARetry(t *testing.T) {
	var attempts int
	var lastHeader http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		lastHeader = r.Header.Clone()
		if attempts == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	propagator := httpkit.PropagatorFunc(func(ctx context.Context, h http.Header) {
		h.Set("X-Trace-Id", "trace-abc")
	})

	fetcher, err := remotesource.New(srv.URL,
		remotesource.WithTimeout(5*time.Second),
		remotesource.WithPropagator(propagator),
		remotesource.WithRetry(remotesource.RetryPolicy{
			MaxRetries:    2,
			RetryDelay:    time.Millisecond,
			MaxRetryDelay: 10 * time.Millisecond,
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := fetcher.Fetch(context.Background(), parserkit.DefaultMaxBytes); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if attempts < 2 {
		t.Fatalf("attempts = %d, want the first one to have been retried", attempts)
	}
	if v := lastHeader.Get("X-Trace-Id"); v != "trace-abc" {
		t.Errorf("X-Trace-Id on the retried attempt = %q, want trace-abc", v)
	}
}

// A caller that brings its own client configures propagation on that client;
// WithPropagator does not reach into it.
func TestWithClientCarriesItsOwnPropagator(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	client, err := httpkit.NewClient(&httpkit.Options{
		BaseURL: srv.URL,
		Timeout: 5 * time.Second,
		Propagator: httpkit.PropagatorFunc(func(ctx context.Context, h http.Header) {
			h.Set("X-Trace-Id", "from-the-caller")
		}),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	fetcher, err := remotesource.New(srv.URL, remotesource.WithClient(client))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := fetcher.Fetch(context.Background(), parserkit.DefaultMaxBytes); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if v := got.Get("X-Trace-Id"); v != "from-the-caller" {
		t.Errorf("X-Trace-Id = %q, want from-the-caller", v)
	}
}
