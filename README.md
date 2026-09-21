# Parser Kit

[![Go Reference](https://pkg.go.dev/badge/github.com/soulteary/parser-kit/v2.svg)](https://pkg.go.dev/github.com/soulteary/parser-kit/v2)
[![Go Report Card](.github/goreportcard.svg)](.github/goreportcard-report.md)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![codecov](https://codecov.io/gh/soulteary/parser-kit/graph/badge.svg)](https://codecov.io/gh/soulteary/parser-kit)

[中文文档](README_CN.md)

A generic data loader that reads a JSON list from several sources — a local
file, Redis, an HTTP endpoint — in priority order, with fallback or merge
semantics.

## Features

- **Multi-source support**: local files, Redis and remote HTTP endpoints, plus
  anything else you write a `Fetcher` for
- **Priority-based fallback**: fall through to the next source when one fails
- **Merge strategy**: combine every source that answered, deduplicated by key
- **Generic design**: works with any JSON-serializable type, and `LoadOptions[T]`
  makes `KeyFunc` a compile-time concern
- **You link what you load**: the root package depends on nothing outside the
  standard library
- **Size limits**: every source is bounded, and an overrun is reported as
  `ErrSourceTooLarge` rather than as malformed JSON
- **HTTP retry**: exponential backoff with jitter for remote sources
- **Normalization support**: optional post-parse normalization

## Layout

Everything that needs a third-party client lives in a subpackage, so a program
links only what it actually reads from:

| Package | Contents | Cost |
|---|---|---|
| `parser-kit/v2` | the loader, `File`, `BytesFetcher`, `Fetcher` | standard library only |
| `parser-kit/v2/redissource` | the Redis source | `go-redis` |
| `parser-kit/v2/remotesource` | the HTTP source | `http-kit` (and OpenTelemetry) |

Measured for a program that loads from a file, v1.8.0 against v2.0.0: the
binary goes from 9,699,758 to 3,952,636 bytes, linked packages from 248 to 78,
and its `go.sum` from 20 modules to 2. See
[CHANGELOG.md](CHANGELOG.md#200--2026-09-21) for the full table, including
what the split costs a program that does use all three sources.

## Requirements

- **Go 1.27+** (`go.mod` declares `go 1.27.0`)
- `github.com/redis/go-redis/v9` — only if you import `redissource`
- `github.com/soulteary/http-kit` — only if you import `remotesource`

## Installation

```bash
go get github.com/soulteary/parser-kit/v2
```

Upgrading from v1? The import path changes for everyone; see
[Upgrade Notes](#upgrade-notes).

## Usage

### Basic Example

```go
package main

import (
    "context"

    "github.com/redis/go-redis/v9"
    parserkit "github.com/soulteary/parser-kit/v2"
    "github.com/soulteary/parser-kit/v2/redissource"
    "github.com/soulteary/parser-kit/v2/remotesource"
)

type User struct {
    ID    string `json:"id"`
    Email string `json:"email"`
    Phone string `json:"phone"`
}

func main() {
    loader, err := parserkit.NewLoader[User](nil)
    if err != nil {
        panic(err)
    }

    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    remote, err := remotesource.New("https://api.example.com/users",
        remotesource.WithAuthorization("Bearer token"),
    )
    if err != nil {
        panic(err)
    }

    // Lower priority number = tried first.
    users, err := loader.Load(context.Background(),
        parserkit.At(0, redissource.New(rdb, "users:cache")),
        parserkit.At(1, remote),
        parserkit.At(2, parserkit.File("/path/to/users.json")),
    )
    if err != nil {
        panic(err)
    }

    _ = users
}
```

A program with no Redis and no remote endpoint imports neither subpackage, and
links neither dependency:

```go
loader, _ := parserkit.NewLoader[User](nil)
users, err := loader.Load(ctx,
    parserkit.At(0, parserkit.File("/etc/app/users.json")),
    parserkit.At(1, parserkit.BytesFetcher(builtinDefaults)),
)
```

### Individual Source Loading

```go
users, err := loader.LoadOne(ctx, parserkit.File("/path/to/users.json"))
users, err := loader.LoadOne(ctx, redissource.New(rdb, "users:cache"))
users, err := loader.LoadOne(ctx, remote)
```

### Custom Options

```go
opts := &parserkit.LoadOptions[User]{
    MaxBytes:       20 * 1024 * 1024, // 20MB
    AllowEmptyData: true,
}

loader, err := parserkit.NewLoaderWithNormalize[User](opts, func(users []User) []User {
    for i := range users {
        users[i].Phone = strings.TrimSpace(users[i].Phone)
    }
    return users
})
```

Timeouts, retries and TLS belong to the source that has them, so they are
options on that source rather than on the loader:

```go
remote, err := remotesource.New("https://api.example.com/users",
    remotesource.WithTimeout(10*time.Second),
    remotesource.WithRetry(remotesource.RetryPolicy{
        MaxRetries:    5,
        RetryDelay:    2 * time.Second,
        MaxRetryDelay: 30 * time.Second,
    }),
)
```

## Sources

### File Source

```go
parserkit.At(2, parserkit.File("/path/to/data.json"))

// A file that does not exist is an absent source rather than an error, so the
// loader moves on to the next one:
parserkit.At(2, parserkit.File("/path/to/data.json").AllowMissing())
```

### Redis Source

Reads a Redis key holding JSON. The size is checked with `STRLEN` before the
value is read, so an oversized value is refused rather than pulled into memory.

```go
parserkit.At(0, redissource.New(rdb, "data:cache"))
parserkit.At(0, redissource.New(rdb, "data:cache").AllowMissing())
```

`redissource.New` takes a `redissource.Getter` — the two commands the source
issues — not a concrete client:

```go
type Getter interface {
    Get(ctx context.Context, key string) *redis.StringCmd
    StrLen(ctx context.Context, key string) *redis.IntCmd
}
```

`*redis.Client`, `*redis.ClusterClient`, `*redis.Ring` and
`redis.UniversalClient` all satisfy it, so a cluster or Sentinel deployment
works with the same source. A nil client — including a nil `*redis.Client`
stored in the interface, which would otherwise panic — is reported as
`redissource.ErrClientNil`.

### Remote Source

```go
remote, err := remotesource.New("https://api.example.com/data",
    remotesource.WithAuthorization("Bearer token"),
    remotesource.WithHeader("X-Tenant", "acme"),
    remotesource.WithTimeout(5*time.Second),
)
```

Each `remotesource.Fetcher` builds its own HTTP client. To share one connection
pool across several remote sources — or to bring your own mTLS, proxy or
transport configuration — build the client once and pass it in:

```go
client, err := httpkit.NewClient(&httpkit.Options{
    BaseURL: "https://api.example.com",
    Timeout: 5 * time.Second,
})
a, _ := remotesource.New("https://api.example.com/users", remotesource.WithClient(client))
b, _ := remotesource.New("https://api.example.com/groups", remotesource.WithClient(client))
```

### Security note for remote sources

`remotesource` uses the URL **as given** and sends the value from
`WithAuthorization` as an `Authorization` header. `New` checks that the URL is
an absolute `http` or `https` URL and nothing more, so a caller-controlled URL
still makes this an SSRF primitive that forwards your credentials to wherever
the URL points.

Validate the URL before passing it in, and close the DNS-rebinding window on
the dial:

```go
import "github.com/soulteary/cli-kit/validator"

opts := &validator.URLOptions{AllowedSchemes: []string{"https"}}
if err := validator.ValidateURL(remoteURL, opts); err != nil {
    return err
}
// and use validator.SSRFDialControl(opts) on the transport that fetches it,
// passed in with remotesource.WithClient
```

`remotesource.WithInsecureSkipVerify()` disables TLS verification entirely —
development only.

### Writing your own source

A source is anything with a `Fetch` method. `ReadLimited` enforces the byte
budget the way the built-in sources do — it reads one byte past the limit, so
an oversized source is reported as `ErrSourceTooLarge` instead of being
silently truncated into a JSON syntax error.

```go
type s3Source struct {
    bucket, key string
    client      *s3.Client
}

func (s s3Source) Fetch(ctx context.Context, maxBytes int64) ([]byte, error) {
    out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
        Bucket: &s.bucket, Key: &s.key,
    })
    if err != nil {
        return nil, err
    }
    defer out.Body.Close()

    raw, err := parserkit.ReadLimited(out.Body, maxBytes)
    if err != nil {
        return nil, fmt.Errorf("s3://%s/%s: %w", s.bucket, s.key, err)
    }
    return raw, nil
}

users, err := loader.Load(ctx, parserkit.At(0, s3Source{...}))
```

Returning no bytes and no error means the source is absent; the loader reads
that as an empty result, which `AllowEmptyData` then governs.

## Priority System

Sources are processed in priority order:

- Lower priority number = higher priority
- Priority 0 is the highest priority
- If a source fails, the loader automatically tries the next source
- Behavior depends on `LoadStrategy` (see below)

Sorting is **stable**, so two sources sharing a priority keep the order you
passed them in — the same input always produces the same order.

## Load Strategy

### Fallback (default)

`LoadStrategyFallback`: returns data from the **first successful** source. Use
it for cache → remote → file style loading.

### Merge

`LoadStrategyMerge`: **merges** data from every successful source, deduplicated
by `KeyFunc`. The first source to define a key fixes its position; a later one
replaces its value.

```go
opts := parserkit.DefaultLoadOptions[User]()
opts.LoadStrategy = parserkit.LoadStrategyMerge
opts.KeyFunc = func(u User) (string, bool) { return u.Phone, u.Phone != "" } // key, include

loader, err := parserkit.NewLoader[User](opts)
users, err := loader.Load(ctx, sources...)
```

`KeyFunc` is `KeyFunc[T]`, so a mismatched signature is a compile error at the
call site rather than an error returned from `NewLoader`.

## Options Reference

### `LoadOptions[T]`

| Option | Default | Description |
|--------|---------|-------------|
| `MaxBytes` | 10MB | Max bytes any single source may return |
| `AllowEmptyData` | false | When false, treat an empty source as a failure and try the next |
| `LoadStrategy` | `fallback` | `fallback` or `merge` |
| `KeyFunc` | nil | Required for `merge`; `func(T) (string, bool)` |

### `remotesource` options

| Option | Default | Description |
|--------|---------|-------------|
| `WithTimeout(d)` | 5s | Bounds one request, retries included |
| `WithRetry(RetryPolicy{MaxRetries})` | 3 | Retries after a failed request |
| `WithRetry(RetryPolicy{RetryDelay})` | 1s | Delay before the first retry; doubles from there. **Must be positive** — see below |
| `WithRetry(RetryPolicy{MaxRetryDelay})` | 30s | Ceiling on the backoff. **Must be positive** — see below |
| `WithAuthorization(v)` | — | `Authorization` header, used verbatim |
| `WithHeader(k, v)` | — | Extra request header; may be repeated |
| `WithUserAgent(ua)` | — | `User-Agent` for this source's requests |
| `WithInsecureSkipVerify()` | off | Skip TLS verification (dev only) |
| `WithClient(c)` | — | Use an existing `*httpkit.Client` instead of building one |

`RetryDelay` and `MaxRetryDelay` are not optional in the way a zero value
usually is. http-kit computes `RetryDelay × 2^attempt` and clamps the result to
`MaxRetryDelay` unconditionally, so a zero in **either** field means every retry
fires immediately and a failing remote source is retried as fast as the network
allows. `New` replaces a non-positive value in either field with its default,
which is why `WithRetry(RetryPolicy{MaxRetries: 5})` is safe to write.

`NewLoader` and `NewLoaderWithNormalize` work on a **copy** of your
`LoadOptions`, so filling in defaults does not mutate the struct you passed.

## Error Handling

- If every source fails, `Load` returns an error carrying the last one.
- `LoadOne` returns its source's error directly.
- A `Source` with no `Fetcher` reports `ErrNoFetcher`.
- A missing file or Redis key is an error by default; `AllowMissing()` on that
  source makes it an absent source instead, and the loader moves on.

### Oversized sources

A source larger than `MaxBytes` fails with `ErrSourceTooLarge`, from every
source type:

```go
data, err := loader.Load(ctx, sources...)
if errors.Is(err, parserkit.ErrSourceTooLarge) {
    // the source exists and is reachable, but exceeds MaxBytes
}
```

Match it with `errors.Is` rather than inspecting the message — an oversized
source and a malformed one are different problems with different fixes.

Pass `math.MaxInt64` as `MaxBytes` to mean "no limit"; the one-byte overrun
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

Tests do not require a real Redis instance or a real HTTP server. The suite uses
[miniredis](https://github.com/alicebob/miniredis) and `net/http/httptest`, so
you can run tests and coverage locally without any external services:

```bash
go test ./...
go test -race -coverprofile=coverage.out -covermode=atomic ./...
go tool cover -func=coverage.out
```

## Dependencies

The root package has none beyond the standard library.

- `github.com/redis/go-redis/v9` — used by `redissource`
- `github.com/soulteary/http-kit` — used by `remotesource`

Test-only: `github.com/alicebob/miniredis/v2` for in-process Redis, and
`github.com/stretchr/testify`.

Both moved dependencies keep their minimum versions in this module's `go.mod`,
and minimum version selection still passes those minimums to anyone who imports
the subpackages. What the split removes is the requirement for everyone else.

## Upgrade Notes

### v2.0.0

**The import path changes for every user**, including programs that only read
files, because Go encodes the major version in the module path:

```go
import parserkit "github.com/soulteary/parser-kit/v2"
```

The Redis and HTTP sources moved into subpackages so the root package could
stop importing go-redis and http-kit. Deprecated shims were not an option: a
shim for `FromRedis` has to import go-redis, which relinks it and gives the
entire benefit back.

| v1 | v2 |
|---|---|
| `loader.FromFile(ctx, path)` | `loader.LoadOne(ctx, parserkit.File(path))` |
| `loader.FromRedis(ctx, client, key)` | `loader.LoadOne(ctx, redissource.New(client, key))` |
| `loader.FromRemote(ctx, url, auth)` | `loader.LoadOne(ctx, remotesource.New(url, remotesource.WithAuthorization(auth)))` |
| `Source{Type: SourceTypeFile, Priority: n, Config: SourceConfig{FilePath: p}}` | `parserkit.At(n, parserkit.File(p))` |
| `Source{Type: SourceTypeRedis, …RedisClient: c, RedisKey: k}` | `parserkit.At(n, redissource.New(c, k))` |
| `Source{Type: SourceTypeRemote, …RemoteURL: u, AuthorizationHeader: a}` | `parserkit.At(n, remote)` from `remotesource.New(u, remotesource.WithAuthorization(a))` |
| `LoadOptions` | `LoadOptions[T]` |
| `DefaultLoadOptions()` | `DefaultLoadOptions[T]()` |
| `LoadOptions.KeyFunc interface{}` | `LoadOptions[T].KeyFunc KeyFunc[T]` |
| `LoadOptions.MaxFileSize` | `LoadOptions[T].MaxBytes` |
| `LoadOptions.AllowEmptyFile` | `parserkit.File(path).AllowMissing()` |
| `LoadOptions.HTTPTimeout` | `remotesource.WithTimeout(d)` |
| `LoadOptions.MaxRetries` / `.RetryDelay` / `.MaxRetryDelay` | `remotesource.WithRetry(remotesource.RetryPolicy{…})` |
| `LoadOptions.InsecureSkipVerify` | `remotesource.WithInsecureSkipVerify()` |

Also in v2, and easy to miss:

- **A zero `RetryDelay` no longer means "retry immediately".** v1.7.0 fixed this
  for `MaxRetryDelay` and left the same hole one field over, so
  `&LoadOptions{MaxRetries: 3}` retried as fast as the network allowed.
- **A zero-byte source decodes to an empty list** rather than
  "unexpected end of JSON input". With the default `AllowEmptyData: false` a
  fallback chain behaves exactly as before.
- **`redissource` accepts any go-redis client shape.** v1 asserted
  `client.(*redis.Client)` and rejected a `*redis.ClusterClient` at run time.

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
