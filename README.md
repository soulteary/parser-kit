# Parser Kit

[![Go Reference](https://pkg.go.dev/badge/github.com/soulteary/parser-kit.svg)](https://pkg.go.dev/github.com/soulteary/parser-kit)
[![Go Report Card](https://goreportcard.com/badge/github.com/soulteary/parser-kit)](https://goreportcard.com/report/github.com/soulteary/parser-kit)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![codecov](https://codecov.io/gh/soulteary/parser-kit/graph/badge.svg)](https://codecov.io/gh/soulteary/parser-kit)

[中文文档](README_CN.md)

A generic data loader kit that supports loading data from multiple sources (file, Redis, remote HTTP) with priority-based fallback strategy.

## Features

- **Multi-source support**: Load data from local files, Redis, or remote HTTP endpoints
- **Priority-based fallback**: Automatically fallback to next source if previous one fails
- **Generic design**: Works with any JSON-serializable type
- **HTTP retry mechanism**: Automatic retry with exponential backoff for remote requests
- **Size limits**: Protection against memory exhaustion attacks (file/remote; Redis uses MaxFileSize with a size check)
- **Normalization support**: Optional data normalization after parsing

## Installation

```bash
go get github.com/soulteary/parser-kit
```

## Usage

### Basic Example

```go
package main

import (
    "context"
    "github.com/soulteary/parser-kit"
    "github.com/soulteary/redis-kit/client"
)

type User struct {
    ID    string `json:"id"`
    Email string `json:"email"`
    Phone string `json:"phone"`
}

func main() {
    // Create loader
    loader, err := parserkit.NewLoader[User](nil)
    if err != nil {
        panic(err)
    }

    // Define sources with priority (lower number = higher priority)
    sources := []parserkit.Source{
        {
            Type:     parserkit.SourceTypeRedis,
            Priority: 0, // Highest priority
            Config: parserkit.SourceConfig{
                RedisKey:    "users:cache",
                RedisClient: redisClient, // *redis.Client
            },
        },
        {
            Type:     parserkit.SourceTypeRemote,
            Priority: 1,
            Config: parserkit.SourceConfig{
                RemoteURL:           "https://api.example.com/users",
                AuthorizationHeader: "Bearer token",
            },
        },
        {
            Type:     parserkit.SourceTypeFile,
            Priority: 2, // Lowest priority (fallback)
            Config: parserkit.SourceConfig{
                FilePath: "/path/to/users.json",
            },
        },
    }

    // Load data (will try Redis first, then remote, then file)
    ctx := context.Background()
    users, err := loader.Load(ctx, sources...)
    if err != nil {
        panic(err)
    }

    // Use users...
}
```

### Individual Source Loading

```go
// Load from file
users, err := loader.FromFile(ctx, "/path/to/users.json")

// Load from remote
users, err := loader.FromRemote(ctx, "https://api.example.com/users", "Bearer token")

// Load from Redis
users, err := loader.FromRedis(ctx, redisClient, "users:cache")
```

### Custom Options

```go
opts := &parserkit.LoadOptions{
    MaxFileSize:  20 * 1024 * 1024, // 20MB
    MaxRetries:   5,
    RetryDelay:   2 * time.Second,
    HTTPTimeout:  10 * time.Second,
}

normalizeFunc := func(users []User) []User {
    // Normalize data after parsing
    for i := range users {
        // Apply normalization logic
    }
    return users
}

loader, err := parserkit.NewLoaderWithNormalize[User](opts, normalizeFunc)
```

## Source Types

### File Source

Loads data from a local JSON file.

```go
{
    Type: parserkit.SourceTypeFile,
    Priority: 2,
    Config: parserkit.SourceConfig{
        FilePath: "/path/to/data.json",
    },
}
```

### Redis Source

Loads data from a Redis key (must contain JSON).

```go
{
    Type: parserkit.SourceTypeRedis,
    Priority: 0,
    Config: parserkit.SourceConfig{
        RedisKey:    "data:cache",
        RedisClient: redisClient, // *redis.Client from redis-kit
    },
}
```

### Remote Source

Loads data from a remote HTTP/HTTPS endpoint.
Make sure `RemoteURL` is trusted or validated by the caller to avoid SSRF.

```go
{
    Type: parserkit.SourceTypeRemote,
    Priority: 1,
    Config: parserkit.SourceConfig{
        RemoteURL:           "https://api.example.com/data",
        AuthorizationHeader: "Bearer token", // Optional
        Timeout:             5 * time.Second, // Optional, uses default if not set
    },
}
// Note: InsecureSkipVerify is set in LoadOptions at loader creation, not per source.
```
> Note: `InsecureSkipVerify` is applied at loader creation time via `LoadOptions`. Per-source values are ignored; create separate loaders if you need different TLS behavior per source.

## Priority System

Sources are processed in priority order:
- Lower priority number = higher priority
- Priority 0 is the highest priority
- If a source fails, the loader automatically tries the next source
- Behavior depends on LoadStrategy (see below)

## Load Strategy

Two strategies control how data from multiple sources is combined:

### Fallback (default)

`LoadStrategyFallback`: Returns data from the **first successful** source. Use for cache → remote → file style loading.

### Merge

`LoadStrategyMerge`: **Merges** data from all successful sources with deduplication. Use when you need "remote + local supplement" (e.g. Warden's REMOTE_FIRST). Requires `KeyFunc` to extract a unique key per item.

```go
keyFunc := func(u User) (string, bool) { return u.Phone, true } // key, include
opts := parserkit.DefaultLoadOptions()
opts.LoadStrategy = parserkit.LoadStrategyMerge
opts.KeyFunc = keyFunc
loader, _ := parserkit.NewLoader[User](opts)

// Load merges file1 + file2; same key overwrites (later source wins)
users, _ := loader.Load(ctx, sources...)
```

## Options Reference

| Option | Default | Description |
|--------|---------|-------------|
| `MaxFileSize` | 10MB | Max bytes to read from file/response |
| `MaxRetries` | 3 | Retries for remote requests |
| `RetryDelay` | 1s | Base delay between retries |
| `HTTPTimeout` | 5s | Timeout for remote requests |
| `InsecureSkipVerify` | false | Skip TLS verification (dev only) |
| `AllowEmptyFile` | false | Return `[]` when file not found instead of error |
| `AllowEmptyData` | false | When false, treat empty source as failure and try next |
| `LoadStrategy` | `fallback` | `fallback` or `merge` |
| `KeyFunc` | nil | Required for `merge`; `func(T) (string, bool)` |

Use `DefaultLoadOptions()` and override fields as needed so `MaxFileSize` and similar are set.
`MaxFileSize` is also used as a Redis value size guard before loading.

## Error Handling

- If all sources fail, `Load()` returns an error with the last error encountered
- Individual source methods (`FromFile`, `FromRemote`, `FromRedis`) return errors immediately
- File not found: error by default; use `AllowEmptyFile: true` to return `[]`

## Testing

Tests do not require a real Redis instance. The suite uses [miniredis](https://github.com/alicebob/miniredis) for Redis-related tests, so you can run tests and coverage locally without any external services:

```bash
go test ./...
go test -coverprofile=coverage.out -covermode=atomic ./...
go tool cover -func=coverage.out
```

## Dependencies

- `github.com/soulteary/http-kit` - For HTTP client and retry logic
- `github.com/redis/go-redis/v9` - For Redis operations

Test-only: `github.com/alicebob/miniredis/v2` for in-process Redis in tests.

## License

See LICENSE file for details.
