package parserkit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"

	"github.com/redis/go-redis/v9"
	"github.com/soulteary/http-kit"
)

// loader is the default implementation of DataLoader
type loader[T any] struct {
	options       *LoadOptions
	client        *httpkit.Client
	normalizeFunc NormalizeFunc[T]
	keyFunc       func(T) (string, bool) // Extracted from LoadOptions.KeyFunc
}

// NewLoader creates a new generic data loader
func NewLoader[T any](opts *LoadOptions) (DataLoader[T], error) {
	return NewLoaderWithNormalize[T](opts, nil)
}

// NewLoaderWithNormalize creates a new generic data loader with normalization function
func NewLoaderWithNormalize[T any](opts *LoadOptions, normalizeFunc NormalizeFunc[T]) (DataLoader[T], error) {
	if opts == nil {
		opts = DefaultLoadOptions()
	} else {
		// Work on a copy: mutating the caller's struct surprises anyone who
		// reuses or shares it.
		cloned := *opts
		opts = &cloned

		// MaxFileSize 0 would cause io.LimitReader to read 0 bytes; use default when unset
		if opts.MaxFileSize == 0 {
			opts.MaxFileSize = DefaultLoadOptions().MaxFileSize
		}
		if opts.LoadStrategy == "" {
			opts.LoadStrategy = LoadStrategyFallback
		}
		// A zero MaxRetryDelay is not "no ceiling": http-kit clamps every
		// computed delay to it, so leaving it unset removed the backoff
		// entirely and retried a failing source as fast as the network allowed.
		if opts.MaxRetryDelay <= 0 {
			opts.MaxRetryDelay = DefaultLoadOptions().MaxRetryDelay
		}
	}

	// Validate LoadStrategy and KeyFunc
	if opts.LoadStrategy == LoadStrategyMerge {
		if opts.KeyFunc == nil {
			return nil, fmt.Errorf("KeyFunc is required when LoadStrategy is LoadStrategyMerge")
		}
	}

	// Create HTTP client for remote requests
	// BaseURL is required by http-kit but we use full URLs in requests
	httpOpts := &httpkit.Options{
		BaseURL:            "http://localhost", // Placeholder, not used since we use full URLs
		Timeout:            opts.HTTPTimeout,
		InsecureSkipVerify: opts.InsecureSkipVerify,
	}
	client, err := httpkit.NewClient(httpOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %w", err)
	}

	// Extract keyFunc from interface{} via type assertion (KeyFunc must be func(T) (string, bool))
	var keyFunc func(T) (string, bool)
	if opts.KeyFunc != nil {
		if kf, ok := opts.KeyFunc.(func(T) (string, bool)); ok {
			keyFunc = kf
		} else {
			return nil, fmt.Errorf("KeyFunc must be of type func(T) (string, bool) for LoadStrategyMerge")
		}
	}

	return &loader[T]{
		options:       opts,
		client:        client,
		normalizeFunc: normalizeFunc,
		keyFunc:       keyFunc,
	}, nil
}

// retryOptions builds the retry policy handed to http-kit.
//
// MaxRetryDelay is load-bearing: http-kit clamps each computed delay to it
// unconditionally, so omitting it made every retry immediate.
func (l *loader[T]) retryOptions() *httpkit.RetryOptions {
	return &httpkit.RetryOptions{
		MaxRetries:    l.options.MaxRetries,
		RetryDelay:    l.options.RetryDelay,
		MaxRetryDelay: l.options.MaxRetryDelay,

		BackoffMultiplier: 2.0,
		RetryableStatusCodes: []int{
			http.StatusRequestTimeout,
			http.StatusTooManyRequests,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout,
		},
	}
}

// readLimit is the byte budget handed to io.LimitReader: one past MaxFileSize,
// so reading the extra byte makes an overrun detectable rather than silently
// truncating. It saturates instead of wrapping, because MaxFileSize set to
// math.MaxInt64 -- the natural way to say "no limit" -- would otherwise
// overflow to math.MinInt64 and make LimitReader return EOF immediately.
func readLimit(maxSize int64) int64 {
	// == rather than >=: for an int64 the two are equivalent here, and
	// staticcheck rightly points out that nothing exceeds math.MaxInt64.
	if maxSize == math.MaxInt64 {
		return math.MaxInt64
	}
	return maxSize + 1
}

// FromFile loads data from a local file
func (l *loader[T]) FromFile(ctx context.Context, path string) ([]T, error) {
	// Check if file exists
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			if l.options.AllowEmptyFile {
				return []T{}, nil // Return empty slice instead of error
			}
			return nil, fmt.Errorf("file not found: %s", path)
		}
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Open file
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Read with the size limit, and distinguish "too large" from "malformed".
	//
	// io.LimitReader truncates silently, so an oversized file surfaced as a
	// JSON parse error with no way to tell the two apart. Reading one byte
	// past the limit makes the overrun detectable.
	raw, err := io.ReadAll(io.LimitReader(file, readLimit(l.options.MaxFileSize)))
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	if int64(len(raw)) > l.options.MaxFileSize {
		return nil, fmt.Errorf("%w: file %q exceeds MaxFileSize (%d bytes)", ErrSourceTooLarge, path, l.options.MaxFileSize)
	}

	// Parse JSON
	var data []T
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Normalize if function provided
	if l.normalizeFunc != nil {
		data = l.normalizeFunc(data)
	}

	return data, nil
}

// FromRemote loads data from a remote URL.
//
// SECURITY: url is used as given and auth is sent as an Authorization header.
// Nothing here validates the destination, so passing a caller-controlled URL
// makes this an SSRF primitive that forwards credentials. Validate untrusted
// URLs before calling -- cli-kit's validator.ValidateURL plus
// validator.SSRFDialControl on the transport covers both the name and the
// address it actually resolves to at connect time.
func (l *loader[T]) FromRemote(ctx context.Context, url, auth string) ([]T, error) {
	// Create request
	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cache-Control", "max-age=0")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	// Inject trace context if available
	l.client.InjectTraceContext(ctx, req)

	// Execute request with retry
	resp, err := l.client.DoRequestWithRetry(ctx, req, l.retryOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch remote data: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Read with the size limit; see FromFile on why one extra byte is read.
	body, err := io.ReadAll(io.LimitReader(resp.Body, readLimit(l.options.MaxFileSize)))
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	if int64(len(body)) > l.options.MaxFileSize {
		return nil, fmt.Errorf("%w: response from %q exceeds MaxFileSize (%d bytes)", ErrSourceTooLarge, url, l.options.MaxFileSize)
	}

	// Parse JSON
	var data []T
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Normalize if function provided
	if l.normalizeFunc != nil {
		data = l.normalizeFunc(data)
	}

	return data, nil
}

// FromRedis loads data from Redis
func (l *loader[T]) FromRedis(ctx context.Context, client interface{}, key string) ([]T, error) {
	// Type assert to redis.Client
	redisClient, ok := client.(*redis.Client)
	if !ok {
		return nil, fmt.Errorf("invalid Redis client type")
	}

	// Enforce MaxFileSize for Redis values to prevent memory exhaustion
	if l.options.MaxFileSize > 0 {
		size, err := redisClient.StrLen(ctx, key).Result()
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("failed to get Redis value size: %w", err)
		}
		if err == nil && size > l.options.MaxFileSize {
			return nil, fmt.Errorf("%w: redis key %q is %d bytes, over MaxFileSize (%d bytes)", ErrSourceTooLarge, key, size, l.options.MaxFileSize)
		}
	}

	// Get data from Redis
	val, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("key not found in Redis: %s", key)
		}
		return nil, fmt.Errorf("failed to get from Redis: %w", err)
	}

	// Parse JSON
	var data []T
	if err := json.Unmarshal([]byte(val), &data); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Normalize if function provided
	if l.normalizeFunc != nil {
		data = l.normalizeFunc(data)
	}

	return data, nil
}

// Load loads data from multiple sources with priority-based fallback or merge
func (l *loader[T]) Load(ctx context.Context, sources ...Source) ([]T, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("no sources provided")
	}

	// Sort sources by priority (lower number = higher priority)
	sortedSources := make([]Source, len(sources))
	copy(sortedSources, sources)
	// Stable: sort.Slice is not, so two sources sharing a priority took an
	// arbitrary order that could differ between runs on identical input.
	sort.SliceStable(sortedSources, func(i, j int) bool {
		return sortedSources[i].Priority < sortedSources[j].Priority
	})

	// Determine strategy
	strategy := l.options.LoadStrategy
	if strategy == "" {
		strategy = LoadStrategyFallback
	}

	if strategy == LoadStrategyMerge {
		return l.loadWithMerge(ctx, sortedSources)
	}

	// Fallback strategy: return first successful source
	return l.loadWithFallback(ctx, sortedSources)
}

// loadWithFallback implements fallback strategy: returns data from first successful source.
//
// NOTE on AllowEmptyData: when it is false, a source that returns successfully
// but with zero rows is treated as a failure and the next source is tried.
// That is dangerous for allow/deny lists: clearing the primary source falls
// back to a stale copy in Redis or on a remote, so entries that were just
// deleted come back. Set AllowEmptyData when an empty result is a legitimate
// state -- which for a policy list it almost always is.
func (l *loader[T]) loadWithFallback(ctx context.Context, sources []Source) ([]T, error) {
	var lastErr error
	for _, source := range sources {
		data, err := l.loadFromSource(ctx, source)
		if err != nil {
			lastErr = err
			continue
		}

		// If successful, check if we should return or continue
		if len(data) > 0 {
			return data, nil
		}
		if !l.options.AllowEmptyData {
			// Empty data but no error - continue to next source
			lastErr = fmt.Errorf("source returned empty data")
			continue
		}
		// AllowEmptyData is true, return empty data
		return data, nil
	}

	// All sources failed
	return nil, fmt.Errorf("all sources failed, last error: %w", lastErr)
}

// loadWithMerge implements merge strategy: merges data from all successful sources with deduplication
func (l *loader[T]) loadWithMerge(ctx context.Context, sources []Source) ([]T, error) {
	if l.keyFunc == nil {
		return nil, fmt.Errorf("KeyFunc is required for merge strategy")
	}

	// Use map for deduplication and slice for order
	dict := make(map[string]T)
	order := make([]string, 0)
	var lastErr error
	hasData := false

	for _, source := range sources {
		data, err := l.loadFromSource(ctx, source)
		if err != nil {
			lastErr = err
			continue
		}

		// Merge data into dict
		for _, item := range data {
			key, include := l.keyFunc(item)
			if !include {
				continue
			}
			if _, exists := dict[key]; !exists {
				order = append(order, key)
			}
			dict[key] = item
			hasData = true
		}
	}

	if !hasData {
		if lastErr != nil {
			return nil, fmt.Errorf("all sources failed, last error: %w", lastErr)
		}
		if !l.options.AllowEmptyData {
			return nil, fmt.Errorf("all sources returned empty data")
		}
		return []T{}, nil
	}

	// Convert map back to slice maintaining order
	result := make([]T, 0, len(order))
	for _, key := range order {
		if item, exists := dict[key]; exists {
			result = append(result, item)
		}
	}

	return result, nil
}

// loadFromSource loads data from a single source
func (l *loader[T]) loadFromSource(ctx context.Context, source Source) ([]T, error) {
	switch source.Type {
	case SourceTypeFile:
		if source.Config.FilePath == "" {
			return nil, fmt.Errorf("file path not specified")
		}
		return l.FromFile(ctx, source.Config.FilePath)

	case SourceTypeRedis:
		if source.Config.RedisKey == "" || source.Config.RedisClient == nil {
			return nil, fmt.Errorf("redis key or client not specified")
		}
		return l.FromRedis(ctx, source.Config.RedisClient, source.Config.RedisKey)

	case SourceTypeRemote:
		if source.Config.RemoteURL == "" {
			return nil, fmt.Errorf("remote URL not specified")
		}
		timeout := source.Config.Timeout
		if timeout == 0 {
			timeout = l.options.HTTPTimeout
		}
		// Create a context with timeout if specified
		loadCtx := ctx
		if timeout > 0 {
			var cancel context.CancelFunc
			loadCtx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		// Note: InsecureSkipVerify is set at client creation time in NewLoader.
		// If source.Config.InsecureSkipVerify is set, it will be ignored in favor of the global option.
		// For per-source TLS configuration, create separate loader instances.
		return l.FromRemote(loadCtx, source.Config.RemoteURL, source.Config.AuthorizationHeader)

	default:
		return nil, fmt.Errorf("unknown source type: %s", source.Type)
	}
}
