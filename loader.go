package parserkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// loader is the default implementation of DataLoader
type loader[T any] struct {
	options       *LoadOptions[T]
	normalizeFunc NormalizeFunc[T]
}

// NewLoader creates a new generic data loader
func NewLoader[T any](opts *LoadOptions[T]) (DataLoader[T], error) {
	return NewLoaderWithNormalize[T](opts, nil)
}

// NewLoaderWithNormalize creates a new generic data loader with normalization function
func NewLoaderWithNormalize[T any](opts *LoadOptions[T], normalizeFunc NormalizeFunc[T]) (DataLoader[T], error) {
	if opts == nil {
		opts = DefaultLoadOptions[T]()
	} else {
		// Work on a copy: mutating the caller's struct surprises anyone who
		// reuses or shares it.
		cloned := *opts
		opts = &cloned

		// MaxBytes 0 would cause io.LimitReader to read 0 bytes; use default when unset
		if opts.MaxBytes <= 0 {
			opts.MaxBytes = DefaultMaxBytes
		}
		if opts.LoadStrategy == "" {
			opts.LoadStrategy = LoadStrategyFallback
		}
	}

	if opts.LoadStrategy == LoadStrategyMerge && opts.KeyFunc == nil {
		return nil, fmt.Errorf("KeyFunc is required when LoadStrategy is LoadStrategyMerge")
	}

	return &loader[T]{
		options:       opts,
		normalizeFunc: normalizeFunc,
	}, nil
}

// LoadOne loads data from a single source.
func (l *loader[T]) LoadOne(ctx context.Context, fetcher Fetcher) ([]T, error) {
	if fetcher == nil {
		return nil, ErrNoFetcher
	}

	raw, err := fetcher.Fetch(ctx, l.options.MaxBytes)
	if err != nil {
		return nil, err
	}
	// Re-checked here rather than trusted: the budget is only as good as the
	// least careful Fetcher, and a third-party one that ignores it should
	// fail loudly instead of quietly handing the decoder an unbounded buffer.
	if int64(len(raw)) > l.options.MaxBytes {
		return nil, fmt.Errorf("%w: fetcher returned %d bytes, over MaxBytes (%d bytes)",
			ErrSourceTooLarge, len(raw), l.options.MaxBytes)
	}

	return l.decode(raw)
}

// decode parses one payload and applies the normalization function.
//
// No bytes means an absent source, which is an empty result rather than a
// JSON syntax error; AllowEmptyData decides what the loader then does with it.
func (l *loader[T]) decode(raw []byte) ([]T, error) {
	data := []T{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &data); err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		if data == nil { // JSON null
			data = []T{}
		}
	}

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
		data, err := l.LoadOne(ctx, source.Fetcher)
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
			lastErr = errors.New("source returned empty data")
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
	if l.options.KeyFunc == nil {
		return nil, fmt.Errorf("KeyFunc is required for merge strategy")
	}

	// Use map for deduplication and slice for order
	dict := make(map[string]T)
	order := make([]string, 0)
	var lastErr error
	hasData := false

	for _, source := range sources {
		data, err := l.LoadOne(ctx, source.Fetcher)
		if err != nil {
			lastErr = err
			continue
		}

		// Merge data into dict
		for _, item := range data {
			key, include := l.options.KeyFunc(item)
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
