package remotesource_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	httpkit "github.com/soulteary/http-kit/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	parserkit "github.com/soulteary/parser-kit/v2"
	"github.com/soulteary/parser-kit/v2/remotesource"
)

type TestUser struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

func newLoader(t *testing.T) parserkit.DataLoader[TestUser] {
	t.Helper()
	l, err := parserkit.NewLoader[TestUser](nil)
	require.NoError(t, err)
	return l
}

func serve(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func newFetcher(t *testing.T, url string, opts ...remotesource.Option) *remotesource.Fetcher {
	t.Helper()
	f, err := remotesource.New(url, opts...)
	require.NoError(t, err)
	return f
}

func TestNew_RejectsUnusableURLs(t *testing.T) {
	for name, url := range map[string]string{
		"empty":         "",
		"relative":      "/rules.json",
		"wrong scheme":  "ftp://example.com/rules.json",
		"scheme only":   "https://",
		"control chars": "http://exa\x7fmple.com",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := remotesource.New(url)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "remotesource:")
		})
	}
}

func TestNew_AcceptsHTTPAndHTTPS(t *testing.T) {
	for _, url := range []string{"http://example.com/rules.json", "https://example.com/rules.json"} {
		f, err := remotesource.New(url)
		require.NoError(t, err)
		assert.Equal(t, url, f.URL())
	}
}

func TestFetch_Success(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		_, _ = w.Write([]byte(`[{"id":"1","email":"a@example.com"}]`))
	})

	users, err := newLoader(t).LoadOne(context.Background(), newFetcher(t, srv.URL))
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "a@example.com", users[0].Email)
}

func TestFetch_WithAuthorizationAndHeaders(t *testing.T) {
	var gotAuth, gotCustom string
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCustom = r.Header.Get("X-Tenant")
		_, _ = w.Write([]byte(`[]`))
	})

	f := newFetcher(t, srv.URL,
		remotesource.WithAuthorization("Bearer token123"),
		remotesource.WithHeader("X-Tenant", "acme"),
	)
	_, err := f.Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.NoError(t, err)

	assert.Equal(t, "Bearer token123", gotAuth)
	assert.Equal(t, "acme", gotCustom)
}

func TestFetch_UserAgent(t *testing.T) {
	var gotUA string
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		_, _ = w.Write([]byte(`[]`))
	})

	f := newFetcher(t, srv.URL, remotesource.WithUserAgent("parser-kit-test/1"))
	_, err := f.Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.NoError(t, err)
	assert.Equal(t, "parser-kit-test/1", gotUA)
}

func TestFetch_Non200(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	_, err := newFetcher(t, srv.URL).Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected status code: 404")
}

func TestFetch_InvalidJSON(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{not json`))
	})

	_, err := newLoader(t).LoadOne(context.Background(), newFetcher(t, srv.URL))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse JSON")
	assert.NotErrorIs(t, err, parserkit.ErrSourceTooLarge)
}

// An oversized response must be distinguishable from a malformed one.
func TestFetch_ResponseTooLarge(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"` + strings.Repeat("x", 512) + `"}]`))
	})

	_, err := newFetcher(t, srv.URL).Fetch(context.Background(), 16)
	require.Error(t, err)
	assert.ErrorIs(t, err, parserkit.ErrSourceTooLarge)
	assert.NotContains(t, err.Error(), "failed to parse JSON")
}

func TestFetch_ContextCancelled(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newFetcher(t, srv.URL).Fetch(ctx, parserkit.DefaultMaxBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch remote data")
}

// A 200 whose body is cut short is a read failure, not malformed JSON and not
// an oversize.
func TestFetch_FailedReadResponse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		c, e := ln.Accept()
		if e != nil {
			return
		}
		defer func() { _ = c.Close() }()
		buf := make([]byte, 4096)
		_, _ = c.Read(buf)
		// Promise 10 bytes, send 2, hang up.
		_, _ = c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 10\r\n\r\n[]"))
	}()

	f := newFetcher(t, "http://"+ln.Addr().String(),
		remotesource.WithRetry(remotesource.RetryPolicy{MaxRetries: 0}))
	_, err = f.Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read response")
}

func TestFetch_TimeoutBoundsTheRequest(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
		_, _ = w.Write([]byte(`[]`))
	})

	f := newFetcher(t, srv.URL,
		remotesource.WithTimeout(100*time.Millisecond),
		remotesource.WithRetry(remotesource.RetryPolicy{MaxRetries: 0}),
	)

	start := time.Now()
	_, err := f.Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 1500*time.Millisecond)
}

func TestFetch_SharedClient(t *testing.T) {
	var hits int32
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		_, _ = w.Write([]byte(`[]`))
	})

	client, err := httpkit.NewClient(&httpkit.Options{BaseURL: srv.URL, Timeout: time.Second})
	require.NoError(t, err)

	for _, path := range []string{"/a.json", "/b.json"} {
		f := newFetcher(t, srv.URL+path, remotesource.WithClient(client))
		_, err := f.Fetch(context.Background(), parserkit.DefaultMaxBytes)
		require.NoError(t, err)
	}
	assert.EqualValues(t, 2, atomic.LoadInt32(&hits))
}

// --- retry policy ---

func TestDefaultRetryPolicy(t *testing.T) {
	p := remotesource.DefaultRetryPolicy()
	assert.Equal(t, 3, p.MaxRetries)
	assert.Equal(t, 1*time.Second, p.RetryDelay)
	assert.Equal(t, 30*time.Second, p.MaxRetryDelay)

	assert.Equal(t, p, newFetcher(t, "https://example.com/x.json").Retry())
}

// http-kit clamps every computed delay to MaxRetryDelay unconditionally, so a
// zero there is not "no ceiling" -- it removes the backoff entirely and turns
// a failing source into a tight request loop. Same for a zero RetryDelay,
// which multiplies to zero however many times it is doubled.
func TestRetryPolicy_NonPositiveValuesTakeDefaults(t *testing.T) {
	f := newFetcher(t, "https://example.com/x.json", remotesource.WithRetry(remotesource.RetryPolicy{
		MaxRetries:    2,
		RetryDelay:    0,
		MaxRetryDelay: 0,
	}))

	got := f.Retry()
	assert.Equal(t, 2, got.MaxRetries)
	assert.Equal(t, 1*time.Second, got.RetryDelay)
	assert.Equal(t, 30*time.Second, got.MaxRetryDelay)
}

func TestRetryPolicy_ExplicitValuesAreKept(t *testing.T) {
	f := newFetcher(t, "https://example.com/x.json", remotesource.WithRetry(remotesource.RetryPolicy{
		MaxRetries:    5,
		RetryDelay:    250 * time.Millisecond,
		MaxRetryDelay: 2 * time.Second,
	}))

	assert.Equal(t, remotesource.RetryPolicy{
		MaxRetries:    5,
		RetryDelay:    250 * time.Millisecond,
		MaxRetryDelay: 2 * time.Second,
	}, f.Retry())
}

func TestRetryPolicy_NegativeRetriesMeansNone(t *testing.T) {
	f := newFetcher(t, "https://example.com/x.json",
		remotesource.WithRetry(remotesource.RetryPolicy{MaxRetries: -1}))
	assert.Equal(t, 0, f.Retry().MaxRetries)
}

// WithRetry must not be read back out of the caller's struct later.
func TestRetryPolicy_CallerStructIsNotMutated(t *testing.T) {
	p := remotesource.RetryPolicy{MaxRetries: 2}
	newFetcher(t, "https://example.com/x.json", remotesource.WithRetry(p))

	assert.Zero(t, p.RetryDelay)
	assert.Zero(t, p.MaxRetryDelay)
}

// The delays are real: a retried request does not fire again immediately.
func TestRetry_BacksOff(t *testing.T) {
	var hits int32
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	f := newFetcher(t, srv.URL, remotesource.WithRetry(remotesource.RetryPolicy{
		MaxRetries:    2,
		RetryDelay:    80 * time.Millisecond,
		MaxRetryDelay: time.Second,
	}))

	start := time.Now()
	_, err := f.Fetch(context.Background(), parserkit.DefaultMaxBytes)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.EqualValues(t, 3, atomic.LoadInt32(&hits), "one attempt plus two retries")
	// 80ms then 160ms, less up to 20% jitter on each.
	assert.GreaterOrEqual(t, elapsed, 150*time.Millisecond)
}

func TestRetry_RecoversOnALaterAttempt(t *testing.T) {
	var hits int32
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&hits, 1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`[{"id":"1"}]`))
	})

	f := newFetcher(t, srv.URL, remotesource.WithRetry(remotesource.RetryPolicy{
		MaxRetries:    2,
		RetryDelay:    10 * time.Millisecond,
		MaxRetryDelay: time.Second,
	}))

	users, err := newLoader(t).LoadOne(context.Background(), f)
	require.NoError(t, err)
	assert.Len(t, users, 1)
	assert.EqualValues(t, 2, atomic.LoadInt32(&hits))
}

var _ parserkit.Fetcher = (*remotesource.Fetcher)(nil)

// The TLS opt-out is the difference between a self-signed endpoint answering
// and failing certificate verification, so it has to actually reach the
// transport.
func TestFetch_InsecureSkipVerify(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"1"}]`))
	}))
	t.Cleanup(srv.Close)

	noRetry := remotesource.WithRetry(remotesource.RetryPolicy{MaxRetries: 0})

	_, err := newFetcher(t, srv.URL, noRetry).Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.Error(t, err, "a self-signed certificate must not verify by default")

	raw, err := newFetcher(t, srv.URL, noRetry, remotesource.WithInsecureSkipVerify()).
		Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"id":"1"}]`, string(raw))
}
