// Package redissource loads parser-kit data from a Redis key.
//
// It lives in its own package so that importing the root package does not drag
// go-redis -- and with it cespare/xxhash and go.uber.org/atomic -- into
// binaries that never read from Redis. A service whose rules live on disk or
// behind HTTP pays nothing for Redis support existing; only importing this
// package links it in.
//
//	loader, err := parserkit.NewLoader[Rule](nil)
//	rules, err := loader.Load(ctx,
//		parserkit.At(0, redissource.New(rdb, "app:rules")),
//		parserkit.At(1, parserkit.File("/etc/app/rules.json")),
//	)
//
// The source is a translation layer and nothing more: decoding, priority,
// merge semantics and the byte budget all live in the root package.
package redissource

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/redis/go-redis/v9"

	parserkit "github.com/soulteary/parser-kit/v2"
)

// Getter is the part of a go-redis client this source uses. *redis.Client,
// *redis.ClusterClient, *redis.Ring and redis.UniversalClient all satisfy it,
// so a source written against it works the same on a standalone server, a
// cluster and a Sentinel failover setup.
//
// This replaces v1's `RedisClient interface{}`, which accepted anything, gave
// up at run time on a *redis.ClusterClient, and told the compiler nothing.
type Getter interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	StrLen(ctx context.Context, key string) *redis.IntCmd
}

// ErrClientNil reports that there is no Redis client to read from.
var ErrClientNil = errors.New("redissource: redis client is nil")

// Fetcher reads a JSON payload from one Redis key. It implements
// parserkit.Fetcher.
type Fetcher struct {
	client       Getter
	key          string
	allowMissing bool
}

// Compile-time proof that a Fetcher is usable as a source.
var _ parserkit.Fetcher = (*Fetcher)(nil)

// New returns a Fetcher reading key from client.
func New(client Getter, key string) *Fetcher {
	return &Fetcher{client: client, key: key}
}

// AllowMissing makes a key that does not exist report an absent source rather
// than an error, so the loader moves on to the next source without the missing
// key showing up as the reason everything failed.
//
// A key that exists but holds an empty string is still empty data, not an
// absent source, and a connection failure is still an error.
func (f *Fetcher) AllowMissing() *Fetcher {
	f.allowMissing = true
	return f
}

// Key returns the Redis key this fetcher reads.
func (f *Fetcher) Key() string {
	return f.key
}

// Fetch reads the key, subject to maxBytes.
//
// The size is checked with STRLEN before the value is read, so an oversized
// value is refused rather than pulled into memory first.
func (f *Fetcher) Fetch(ctx context.Context, maxBytes int64) ([]byte, error) {
	if isNil(f.client) {
		return nil, ErrClientNil
	}
	if f.key == "" {
		return nil, errors.New("redissource: redis key not specified")
	}
	if maxBytes <= 0 {
		maxBytes = parserkit.DefaultMaxBytes
	}

	// Enforce the budget before reading, to prevent memory exhaustion.
	size, err := f.client.StrLen(ctx, f.key).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("failed to get Redis value size: %w", err)
	}
	if err == nil && size > maxBytes {
		return nil, fmt.Errorf("redis key %q is %d bytes: %w (limit %d bytes)",
			f.key, size, parserkit.ErrSourceTooLarge, maxBytes)
	}

	val, err := f.client.Get(ctx, f.key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			if f.allowMissing {
				return nil, nil // absent, not a failure
			}
			return nil, fmt.Errorf("key not found in Redis: %s", f.key)
		}
		return nil, fmt.Errorf("failed to get from Redis: %w", err)
	}

	// STRLEN and GET are two round trips, so the value can have grown between
	// them -- and STRLEN reports 0 for a key holding a non-string type rather
	// than failing. Re-check what actually arrived.
	if int64(len(val)) > maxBytes {
		return nil, fmt.Errorf("redis key %q is %d bytes: %w (limit %d bytes)",
			f.key, len(val), parserkit.ErrSourceTooLarge, maxBytes)
	}

	return []byte(val), nil
}

// isNil reports whether there is no client to read from. Getter is an
// interface, so a plain c == nil misses the case that actually reaches here --
// a nil *redis.Client stored in it, which a caller gets from an unassigned
// field or a constructor that returned early. Calling through that panics,
// and taking the process down is worse than reporting the source as failed.
func isNil(c Getter) bool {
	if c == nil {
		return true
	}
	v := reflect.ValueOf(c)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
