package parserkit

import (
	"context"
	"time"
)

// SourceType represents the type of data source
type SourceType string

const (
	// SourceTypeFile represents a local file source
	SourceTypeFile SourceType = "file"
	// SourceTypeRedis represents a Redis source
	SourceTypeRedis SourceType = "redis"
	// SourceTypeRemote represents a remote HTTP/HTTPS source
	SourceTypeRemote SourceType = "remote"
)

// SourceConfig holds configuration for a data source
type SourceConfig struct {
	// For file source
	FilePath string

	// For Redis source
	RedisKey    string
	RedisClient interface{} // *redis.Client from redis-kit

	// For remote source
	RemoteURL           string
	AuthorizationHeader string
	Timeout             time.Duration
	InsecureSkipVerify  bool
}

// Source represents a data source with priority
type Source struct {
	Type     SourceType
	Priority int // Lower number = higher priority (0 is highest)
	Config   SourceConfig
}

// DataLoader is a generic interface for loading data from various sources
type DataLoader[T any] interface {
	// FromFile loads data from a local file
	FromFile(ctx context.Context, path string) ([]T, error)

	// FromRemote loads data from a remote URL
	FromRemote(ctx context.Context, url, auth string) ([]T, error)

	// FromRedis loads data from Redis
	FromRedis(ctx context.Context, client interface{}, key string) ([]T, error)

	// Load loads data from multiple sources
	// Sources are processed in priority order (lower number = higher priority)
	// Behavior depends on LoadStrategy:
	// - LoadStrategyFallback: returns data from the first successful source
	// - LoadStrategyMerge: merges data from all successful sources with deduplication
	Load(ctx context.Context, sources ...Source) ([]T, error)
}

// NormalizeFunc is a function type for normalizing data after parsing
// It accepts a slice of any type and returns a normalized slice
type NormalizeFunc[T any] func([]T) []T

// KeyFunc extracts a unique key from an item for deduplication
// Returns the key and true if the item should be included, false otherwise
type KeyFunc[T any] func(T) (string, bool)

// LoadStrategy determines how data from multiple sources should be combined
type LoadStrategy string

const (
	// LoadStrategyFallback returns data from the first successful source (default)
	LoadStrategyFallback LoadStrategy = "fallback"
	// LoadStrategyMerge merges data from all successful sources with deduplication
	LoadStrategyMerge LoadStrategy = "merge"
)

// LoadOptions configures the behavior of Load operations
type LoadOptions struct {
	// MaxFileSize limits the maximum file size to read (default: 10MB)
	MaxFileSize int64

	// MaxRetries for remote requests (default: 3)
	MaxRetries int

	// RetryDelay for remote requests (default: 1s)
	RetryDelay time.Duration

	// HTTPTimeout for remote requests (default: 5s)
	HTTPTimeout time.Duration

	// InsecureSkipVerify allows skipping TLS certificate verification (default: false)
	// Only use in development environments
	InsecureSkipVerify bool

	// AllowEmptyFile if true, returns empty slice instead of error when file not found (default: false)
	AllowEmptyFile bool

	// AllowEmptyData if true, continues to next source even if current source returns empty data (default: false)
	AllowEmptyData bool

	// LoadStrategy determines how to combine data from multiple sources (default: LoadStrategyFallback)
	// - LoadStrategyFallback: return data from first successful source
	// - LoadStrategyMerge: merge data from all successful sources with deduplication
	LoadStrategy LoadStrategy

	// KeyFunc is required when LoadStrategy is LoadStrategyMerge
	// It extracts a unique key from each item for deduplication
	// Note: This is stored as interface{} and will be type-asserted in the loader
	KeyFunc interface{} // Should be KeyFunc[T] but we can't use generics in struct fields
}

// DefaultLoadOptions returns default load options
func DefaultLoadOptions() *LoadOptions {
	return &LoadOptions{
		MaxFileSize:        10 * 1024 * 1024, // 10MB
		MaxRetries:         3,
		RetryDelay:         1 * time.Second,
		HTTPTimeout:        5 * time.Second,
		InsecureSkipVerify: false,
		AllowEmptyFile:     false,
		AllowEmptyData:     false,
		LoadStrategy:       LoadStrategyFallback,
		KeyFunc:            nil,
	}
}
