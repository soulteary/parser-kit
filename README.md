# Parser Kit

[![Go Reference](https://pkg.go.dev/badge/github.com/soulteary/parser-kit.svg)](https://pkg.go.dev/github.com/soulteary/parser-kit)
[![Go Report Card](.github/goreportcard.svg)](.github/goreportcard-report.md)
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

## Requirements

- **Go 1.27+** (`go.mod` declares `go 1.27.0`)
- `github.com/redis/go-redis/v9` for the Redis source

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

### Security note for remote sources

`FromRemote` uses the URL **as given** and sends `auth` as an `Authorization`
header, with no validation. A caller-controlled URL therefore makes it an SSRF
primitive that forwards your credentials to wherever the URL points.

Validate the URL before passing it in, and close the DNS-rebinding window on the
dial:

```go
import "github.com/soulteary/cli-kit/validator"

opts := &validator.URLOptions{AllowedSchemes: []string{"https"}}
if err := validator.ValidateURL(remoteURL, opts); err != nil {
    return err
}
// and use validator.SSRFDialControl(opts) on the transport that fetches it
```

`InsecureSkipVerify` disables TLS verification entirely — development only.

## Priority System

Sources are processed in priority order:
- Lower priority number = higher priority
- Priority 0 is the highest priority
- If a source fails, the loader automatically tries the next source
- Behavior depends on LoadStrategy (see below)

Sorting is **stable**, so two sources sharing a priority keep the order you
passed them in — the same input always produces the same order.

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
| `MaxRetryDelay` | 30s | Ceiling on the retry backoff. **Must be positive** — see below |
| `HTTPTimeout` | 5s | Timeout for remote requests |
| `InsecureSkipVerify` | false | Skip TLS verification (dev only) |
| `AllowEmptyFile` | false | Return `[]` when file not found instead of error |
| `AllowEmptyData` | false | When false, treat empty source as failure and try next |
| `LoadStrategy` | `fallback` | `fallback` or `merge` |
| `KeyFunc` | nil | Required for `merge`; `func(T) (string, bool)` |

`MaxRetryDelay` is not optional in the way a zero value usually is. http-kit
clamps every computed backoff to it unconditionally, so a zero ceiling means
**every retry fires immediately** — `RetryDelay` and the backoff multiplier look
configured and do nothing, and a failing remote source is retried as fast as the
network allows. `NewLoader` fills the 30s default when it is unset, so this only
bites if you set it to zero on purpose.

Use `DefaultLoadOptions()` and override the fields you need, so `MaxFileSize` and
similar are set rather than zero. `MaxFileSize` also guards the Redis value size
(checked with `STRLEN` before loading).

`NewLoader` and `NewLoaderWithNormalize` work on a **copy** of your
`LoadOptions`, so filling in defaults does not mutate the struct you passed:

```go
opts := parserkit.DefaultLoadOptions()
loader, err := parserkit.NewLoader[User](opts)

// NewLoaderWithNormalize post-processes every successful load
loader, err = parserkit.NewLoaderWithNormalize[User](opts, func(users []User) []User {
    for i := range users {
        users[i].Phone = strings.TrimSpace(users[i].Phone)
    }
    return users
})
```

## Error Handling

- If every source fails, `Load()` returns an error carrying the last one
  encountered.
- The individual source methods (`FromFile`, `FromRemote`, `FromRedis`) return
  their error immediately.
- File not found is an error by default; `AllowEmptyFile: true` returns `[]`.

### Oversized sources

A source larger than `MaxFileSize` fails with `ErrSourceTooLarge`, from all three
source types:

```go
data, err := loader.Load(ctx, sources...)
if errors.Is(err, parserkit.ErrSourceTooLarge) {
    // the source exists and is reachable, but exceeds MaxFileSize
}
```

Match it with `errors.Is` rather than inspecting the message — an oversized
source and a malformed one are different problems with different fixes.

Pass `math.MaxInt64` as `MaxFileSize` to mean "no limit"; the one-byte overrun
margin saturates rather than overflowing.

### An empty source counts as a failure

With `AllowEmptyData: false` (the default), a source that loads successfully but
yields no entries is treated as a **failure**, and the fallback strategy moves on
to the next source.

That is worth thinking about for an allow list or deny list: **clearing the
primary source falls back to a stale copy** in Redis or on a remote, and entries
you just deleted come back. Set `AllowEmptyData: true` when an empty result is a
legitimate state.

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

## Upgrade Notes

### v1.8.0

Dependency refresh only. No API was removed and no call needs rewriting.

- Test Redis is `miniredis` v2.39.0 (was v2.36.1).
- Transitive `yuin/gopher-lua` is v1.1.2 (was v1.1.1), matching the other kits.
- Still requires `http-kit` v1.5.0. That module had no newer published release.

### v1.7.0

- **Remote retries back off again.** `FromRemote` never set `MaxRetryDelay`, and
  http-kit clamps every computed delay to that ceiling *unconditionally* — so a
  ceiling of zero made all three retries fire immediately, turning a failing
  source into a tight request loop. With the defaults the delays are now 1s, 2s
  and 4s rather than none at all. **A remote source that used to fail fast now
  takes a few seconds to exhaust its retries**; lower `RetryDelay` or
  `MaxRetries` if you depended on the old timing.
- **`LoadOptions.MaxRetryDelay` is new** (default 30s). `NewLoader` and
  `NewLoaderWithNormalize` fill it in when you leave it zero, for the reason
  above: a zero you pass is replaced rather than honoured, because it means "no
  backoff", not "no ceiling". Set it explicitly to choose a different ceiling.

### v1.6.0

One sentinel was added; nothing was removed. Two error paths report differently.

- **An oversized source is reported as `ErrSourceTooLarge`, not as malformed
  data.** `io.LimitReader` truncates silently, so a file or HTTP response larger
  than `MaxFileSize` came back as invalid JSON with no way to tell the two apart.
  Both paths now read one byte past the limit and report the sentinel — matching
  `FromRedis`, which already checked `STRLEN` up front, so the three sources
  behaved differently for the same condition. **If you matched on "invalid JSON"
  to detect an oversized source, match `errors.Is(err, ErrSourceTooLarge)`
  instead.**
- **`FromRedis` wraps the sentinel.** It reported an oversized value with a plain
  formatted error, so `errors.Is(err, ErrSourceTooLarge)` was false for the one
  source that had always detected the condition.
- **`MaxFileSize: math.MaxInt64` works.** The one-byte overrun margin
  (`MaxFileSize+1`) wrapped to `math.MinInt64`, `io.LimitReader` returned EOF
  immediately, and **every source — however small — came back as malformed
  JSON**. The limit saturates now.
- **Source ordering is stable.** Sources were ordered with `sort.Slice`, so two
  sources sharing a priority took an arbitrary order that could differ between
  runs on identical input.
- **`NewLoader`/`NewLoaderWithNormalize` no longer mutate your `LoadOptions`.**
  They wrote their defaults back into the caller's struct.
- **Documented, not changed**: an empty-but-successful source counts as a failure
  when `AllowEmptyData` is false — see [An empty source counts as a
  failure](#an-empty-source-counts-as-a-failure) — and `FromRemote` is an SSRF
  primitive for a caller-controlled URL.

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
