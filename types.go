package parserkit

import (
	"context"
	"errors"
)

// NormalizeFunc is a function type for normalizing data after parsing.
// It accepts a slice of any type and returns a normalized slice.
type NormalizeFunc[T any] func([]T) []T

// KeyFunc extracts a unique key from an item for deduplication.
// Returns the key and true if the item should be included, false otherwise.
type KeyFunc[T any] func(T) (string, bool)

// LoadStrategy determines how data from multiple sources should be combined
type LoadStrategy string

const (
	// LoadStrategyFallback returns data from the first successful source (default)
	LoadStrategyFallback LoadStrategy = "fallback"
	// LoadStrategyMerge merges data from all successful sources with deduplication
	LoadStrategyMerge LoadStrategy = "merge"
)

// LoadOptions configures the behavior of Load operations.
//
// It is generic in T so that KeyFunc is checked by the compiler. Timeouts,
// retries and TLS settings are not here: they belong to the source that has
// them, so they are options on that source -- see parserkit/remotesource.
type LoadOptions[T any] struct {
	// MaxBytes limits how much a single source may return (default:
	// DefaultMaxBytes). It applies to every source, not just files: a Fetcher
	// is handed this budget and must report ErrSourceTooLarge rather than
	// return more.
	MaxBytes int64

	// AllowEmptyData if true, a source that succeeds with zero items is a
	// result rather than a failure (default: false).
	//
	// With the default, an empty-but-successful source is treated as a failure
	// and the next source is tried. That is dangerous for allow/deny lists:
	// clearing the primary source falls back to a stale copy elsewhere, so
	// entries that were just deleted come back. Set this when an empty result
	// is a legitimate state -- which for a policy list it almost always is.
	AllowEmptyData bool

	// LoadStrategy determines how to combine data from multiple sources
	// (default: LoadStrategyFallback).
	LoadStrategy LoadStrategy

	// KeyFunc is required when LoadStrategy is LoadStrategyMerge.
	// It extracts a unique key from each item for deduplication.
	KeyFunc KeyFunc[T]
}

// DefaultLoadOptions returns default load options
func DefaultLoadOptions[T any]() *LoadOptions[T] {
	return &LoadOptions[T]{
		MaxBytes:       DefaultMaxBytes,
		AllowEmptyData: false,
		LoadStrategy:   LoadStrategyFallback,
		KeyFunc:        nil,
	}
}

// DataLoader loads a JSON list of T from one or more sources.
type DataLoader[T any] interface {
	// LoadOne loads data from a single source, applying the byte limit and
	// the normalization function but none of the priority or merge handling.
	LoadOne(ctx context.Context, fetcher Fetcher) ([]T, error)

	// Load loads data from multiple sources.
	// Sources are processed in priority order (lower number = higher priority).
	// Behavior depends on LoadStrategy:
	//   - LoadStrategyFallback: returns data from the first successful source
	//   - LoadStrategyMerge: merges data from all successful sources with deduplication
	Load(ctx context.Context, sources ...Source) ([]T, error)
}

// ErrSourceTooLarge reports that a source exceeded LoadOptions.MaxBytes.
//
// It exists so the overrun is distinguishable from malformed content:
// io.LimitReader truncates silently, so an oversized file previously surfaced
// as a JSON parse error.
var ErrSourceTooLarge = errors.New("parserkit: source exceeds MaxBytes")

// ErrNoFetcher reports a Source with no Fetcher in it, which is a programming
// error rather than a source that failed to load.
var ErrNoFetcher = errors.New("parserkit: source has no fetcher")
