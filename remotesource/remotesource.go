// Package remotesource loads parser-kit data from an HTTP endpoint.
//
// It lives in its own package so that importing the root package does not drag
// http-kit into binaries that never fetch anything over HTTP. A service whose
// rules live on disk or in Redis pays nothing for remote support existing;
// only importing this package links it in.
//
// Since http-kit v2 that is all it drags: OpenTelemetry, go-logr and
// golang.org/x/sys are gone from the graph. Trace propagation is now something
// a caller asks for -- see [WithPropagator] -- rather than something every
// importer links whether or not it traces.
//
//	rules, err := remotesource.New("https://config.internal/rules.json",
//		remotesource.WithAuthorization("Bearer "+token),
//		remotesource.WithTimeout(3*time.Second),
//	)
//
// # Security
//
// The URL is used as given and the Authorization header is sent with it.
// Nothing here validates the destination beyond its syntax, so passing a
// caller-controlled URL makes this an SSRF primitive that forwards
// credentials. Validate untrusted URLs before calling -- cli-kit's
// validator.ValidateURL plus validator.SSRFDialControl on the transport
// covers both the name and the address it actually resolves to at connect
// time, which a syntax check cannot.
package remotesource

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	httpkit "github.com/soulteary/http-kit/v2"

	parserkit "github.com/soulteary/parser-kit/v3"
)

// DefaultTimeout bounds a single request when WithTimeout is not used.
const DefaultTimeout = 5 * time.Second

// RetryPolicy is the retry behaviour for one remote source.
//
// In v1 these were fields on LoadOptions, which meant every source in a
// loader shared one policy whether or not any of them were remote.
type RetryPolicy struct {
	// MaxRetries is how many times a failed request is retried (default: 3).
	MaxRetries int

	// RetryDelay is the delay before the first retry (default: 1s). Each
	// subsequent delay doubles, up to MaxRetryDelay.
	//
	// It must be positive. Zero does not mean "retry once immediately" -- it
	// means every retry fires immediately, which turns a failing remote
	// source into a tight request loop, so New replaces a non-positive value
	// with the default.
	RetryDelay time.Duration

	// MaxRetryDelay caps the backoff between retries (default: 30s).
	//
	// It must be positive for the same reason, and more sharply: http-kit's
	// CalculateRetryDelay clamps every computed delay to this ceiling
	// unconditionally, so a zero here removes the backoff however large
	// RetryDelay is. New replaces a non-positive value with the default.
	MaxRetryDelay time.Duration
}

// DefaultRetryPolicy returns the retry policy used when WithRetry is not.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:    3,
		RetryDelay:    1 * time.Second,
		MaxRetryDelay: 30 * time.Second,
	}
}

// normalized returns p with non-positive values replaced by the defaults.
func (p RetryPolicy) normalized() RetryPolicy {
	d := DefaultRetryPolicy()
	if p.MaxRetries < 0 {
		p.MaxRetries = 0
	}
	if p.RetryDelay <= 0 {
		p.RetryDelay = d.RetryDelay
	}
	if p.MaxRetryDelay <= 0 {
		p.MaxRetryDelay = d.MaxRetryDelay
	}
	return p
}

// settings is the configuration an Option writes to, before New turns it into
// a Fetcher.
type settings struct {
	auth               string
	header             http.Header
	timeout            time.Duration
	retry              RetryPolicy
	insecureSkipVerify bool
	client             *httpkit.Client
	userAgent          string
	propagator         httpkit.Propagator
}

// Option configures a Fetcher.
type Option func(*settings)

// WithAuthorization sets the Authorization header sent with the request.
// The value is used verbatim, so include the scheme: "Bearer "+token.
func WithAuthorization(value string) Option {
	return func(s *settings) { s.auth = value }
}

// WithHeader sets an extra request header. It may be used more than once.
func WithHeader(name, value string) Option {
	return func(s *settings) {
		if s.header == nil {
			s.header = http.Header{}
		}
		s.header.Set(name, value)
	}
}

// WithTimeout bounds one request, retries included (default: DefaultTimeout).
// Zero or less leaves the request bounded only by the caller's context.
func WithTimeout(d time.Duration) Option {
	return func(s *settings) { s.timeout = d }
}

// WithRetry sets the retry policy. Non-positive fields take their defaults --
// see RetryPolicy.
func WithRetry(p RetryPolicy) Option {
	return func(s *settings) { s.retry = p }
}

// WithUserAgent sets the User-Agent header for requests this source makes.
// It is ignored when WithClient supplies the client.
func WithUserAgent(ua string) Option {
	return func(s *settings) { s.userAgent = ua }
}

// WithInsecureSkipVerify disables TLS certificate verification.
// Only use it in development. It is ignored when WithClient supplies the
// client, which carries its own TLS configuration.
func WithInsecureSkipVerify() Option {
	return func(s *settings) { s.insecureSkipVerify = true }
}

// WithClient hands the source an http-kit client to use instead of building
// one, so several remote sources can share one connection pool -- and so an
// application that already configures mTLS, a proxy or a custom transport can
// pass that configuration in.
//
// The client's own timeout applies to the transport; WithTimeout still bounds
// the call through the request context.
func WithClient(c *httpkit.Client) Option {
	return func(s *settings) { s.client = c }
}

// WithPropagator injects cross-process context -- trace headers, baggage, a
// request ID -- into every request this source sends. There is none by
// default.
//
// Until http-kit v2 this package called otel.GetTextMapPropagator() on every
// fetch, so a service that configured a global OpenTelemetry propagator got
// trace headers without asking, and every service that imported this package
// linked OpenTelemetry whether it traced or not. To get that behaviour back,
// ask for it:
//
//	import "github.com/soulteary/http-kit/v2/otelprop"
//
//	remotesource.New(url, remotesource.WithPropagator(otelprop.Global()))
//
// It applies to the client this package builds. A caller supplying its own
// through [WithClient] sets Propagator on that client's httpkit.Options
// instead, and this option is then ignored.
func WithPropagator(p httpkit.Propagator) Option {
	return func(s *settings) { s.propagator = p }
}

// Fetcher reads a JSON payload from one HTTP endpoint. It implements
// parserkit.Fetcher.
type Fetcher struct {
	url       string
	auth      string
	header    http.Header
	timeout   time.Duration
	retry     RetryPolicy
	client    *httpkit.Client
	retryOpts *httpkit.RetryOptions
}

// Compile-time proof that a Fetcher is usable as a source.
var _ parserkit.Fetcher = (*Fetcher)(nil)

// New returns a Fetcher reading rawURL.
//
// It fails on a URL that is not an absolute http or https URL, and on a
// client it cannot build -- an unreadable CA file, say, when one was
// configured on a client passed to WithClient. A syntax check is not an SSRF
// defence; see the package documentation.
func New(rawURL string, opts ...Option) (*Fetcher, error) {
	s := settings{
		timeout: DefaultTimeout,
		retry:   DefaultRetryPolicy(),
	}
	for _, opt := range opts {
		opt(&s)
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("remotesource: invalid URL %q: %w", rawURL, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("remotesource: URL %q must be http or https", rawURL)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("remotesource: URL %q has no host", rawURL)
	}

	client := s.client
	if client == nil {
		// Requests here carry a full URL, so BaseURL is only ever the origin
		// this source talks to. http-kit v2 no longer requires one, but
		// setting it keeps GetBaseURL answering usefully for anyone holding
		// the client.
		client, err = httpkit.NewClient(&httpkit.Options{
			BaseURL:            parsed.Scheme + "://" + parsed.Host,
			Timeout:            s.timeout,
			UserAgent:          s.userAgent,
			InsecureSkipVerify: s.insecureSkipVerify,
			Propagator:         s.propagator,
		})
		if err != nil {
			return nil, fmt.Errorf("remotesource: failed to create HTTP client: %w", err)
		}
	}

	retry := s.retry.normalized()
	return &Fetcher{
		url:     rawURL,
		auth:    s.auth,
		header:  s.header,
		timeout: s.timeout,
		retry:   retry,
		client:  client,
		retryOpts: &httpkit.RetryOptions{
			MaxRetries:    retry.MaxRetries,
			RetryDelay:    retry.RetryDelay,
			MaxRetryDelay: retry.MaxRetryDelay,

			BackoffMultiplier: 2.0,
			RetryableStatusCodes: []int{
				http.StatusRequestTimeout,
				http.StatusTooManyRequests,
				http.StatusInternalServerError,
				http.StatusBadGateway,
				http.StatusServiceUnavailable,
				http.StatusGatewayTimeout,
			},
		},
	}, nil
}

// URL returns the endpoint this fetcher reads.
func (f *Fetcher) URL() string {
	return f.url
}

// Retry returns the effective retry policy, after the defaults New filled in.
func (f *Fetcher) Retry() RetryPolicy {
	return f.retry
}

// Fetch performs the request, subject to maxBytes.
func (f *Fetcher) Fetch(ctx context.Context, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = parserkit.DefaultMaxBytes
	}
	if f.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cache-Control", "max-age=0")
	for name, values := range f.header {
		for _, v := range values {
			req.Header.Add(name, v)
		}
	}
	if f.auth != "" {
		req.Header.Set("Authorization", f.auth)
	}

	resp, err := f.client.DoRequestWithRetry(ctx, req, f.retryOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch remote data: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	raw, err := parserkit.ReadLimited(resp.Body, maxBytes)
	if err != nil {
		if errors.Is(err, parserkit.ErrSourceTooLarge) {
			return nil, fmt.Errorf("response from %q: %w", f.url, err)
		}
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	return raw, nil
}
