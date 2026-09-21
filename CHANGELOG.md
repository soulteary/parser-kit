# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Because Go encodes the major version in the import path, every major release
also changes the module path. The current one is
`github.com/soulteary/parser-kit/v2`.

## [Unreleased]

## [2.1.0] — 2026-09-21

### Changed — BEHAVIOUR

- **`remotesource` no longer propagates trace context unless asked.** It used
  to call `otel.GetTextMapPropagator()` on every fetch, through http-kit v1's
  `Client.InjectTraceContext`. Two consequences, and this release trades the
  first away to be rid of the second:

  1. A service that configured a **global** OpenTelemetry propagator got trace
     headers on these requests without asking. It will not any more. **This
     compiles either way and fails silently** — the fetches simply stop
     carrying the headers. If you relied on it, one line brings it back:

     ```go
     import "github.com/soulteary/http-kit/v2/otelprop"

     remotesource.New(url, remotesource.WithPropagator(otelprop.Global()))
     ```

  2. Every service importing `remotesource` linked OpenTelemetry, traced or
     not — six modules for a feature most of them never used.

  A caller supplying its own client through `WithClient` was never affected
  and still is not: it sets `Propagator` on that client's `httpkit.Options`.

### Added

- **`remotesource.WithPropagator`**, which injects cross-process context —
  trace headers, baggage, a request ID — into every request the source sends.
  It takes `httpkit.Propagator`, so `otelprop.Global()` restores the old
  behaviour and a `PropagatorFunc` covers a house convention without pulling
  in OpenTelemetry at all.

  Injection now happens inside http-kit's `Do`, which runs on **every retry
  attempt**. The old call site injected once, before the retry loop, so a
  replayed request went out carrying whatever the first attempt had.

### Dependencies

- **http-kit v1.5.0 → v2.0.0.** Measured for a program importing
  `parser-kit/v2/remotesource`, `-trimpath`, go1.27.0 linux/amd64:

  | | v2.0.0 | v2.1.0 |
  |---|---|---|
  | modules in `go list -m all` | 24 | 17 |
  | `go.sum` lines | 23 | 6 |
  | `// indirect` requirements in the consumer's `go.mod` | 8 | 1 |
  | linked packages | 229 | 202 |
  | binary size | 9,565,564 bytes | 8,169,973 bytes (−14.6%) |

  Six modules leave the graph — `go.opentelemetry.io/otel`, `otel/metric`,
  `otel/trace`, `auto/sdk`, `go-logr/logr` and `go-logr/stdr` — along with
  `golang.org/x/sys`.

  A service that *does* trace pays what it paid before: it imports `otelprop`
  and gets OpenTelemetry back, deliberately.

## [2.0.0] — 2026-09-21

One major release, not three. Every breaking change that was worth making is
in this one, because each major version forces every user to rewrite their
import paths whether or not the change affects them.

### Changed — BREAKING

- **The Redis and HTTP sources moved into subpackages.** The root package no
  longer imports go-redis or http-kit, so it now depends on nothing outside
  the standard library and a binary links only the sources it actually reads
  from.

  | Removed from the root package | Replacement |
  |---|---|
  | `SourceTypeRedis`, `SourceConfig.RedisClient`, `SourceConfig.RedisKey` | `redissource.New(client, key)` |
  | `DataLoader.FromRedis` | `loader.LoadOne(ctx, redissource.New(client, key))` |
  | `SourceTypeRemote`, `SourceConfig.RemoteURL`, `.AuthorizationHeader`, `.Timeout`, `.InsecureSkipVerify` | `remotesource.New(url, opts...)` |
  | `DataLoader.FromRemote` | `loader.LoadOne(ctx, remotesource.New(url, remotesource.WithAuthorization(auth)))` |
  | `SourceTypeFile`, `SourceConfig.FilePath` | `parserkit.File(path)` |
  | `DataLoader.FromFile` | `loader.LoadOne(ctx, parserkit.File(path))` |
  | `SourceType`, `SourceConfig` | `Source{Fetcher, Priority}`, built with `At(priority, fetcher)` |
  | `LoadOptions.AllowEmptyFile` | `parserkit.File(path).AllowMissing()` |
  | `LoadOptions.MaxRetries`, `.RetryDelay`, `.MaxRetryDelay` | `remotesource.WithRetry(remotesource.RetryPolicy{…})` |
  | `LoadOptions.HTTPTimeout` | `remotesource.WithTimeout(d)` |
  | `LoadOptions.InsecureSkipVerify` | `remotesource.WithInsecureSkipVerify()` |
  | `LoadOptions.MaxFileSize` | `LoadOptions.MaxBytes` (it always governed every source, not just files) |

  Measured for a program that loads from a file and imports only the root
  package, v1.8.0 against v2.0.0:

  | | v1.8.0 | v2.0.0 |
  |---|---|---|
  | Binary | 9,699,758 bytes | 3,952,636 bytes (**59.3% smaller**) |
  | Linked packages | 248 | 78 |
  | Modules in its `go.sum` | 20 | 2 |
  | `// indirect` lines in its `go.mod` | 11 | 0 |

  Eighteen modules leave that program's `go.sum`: go-redis, http-kit and
  miniredis, plus the fifteen they drag along — four OpenTelemetry modules,
  go-logr and stdr, cespare/xxhash, klauspost/cpuid, zeebo/xxh3,
  yuin/gopher-lua, go.uber.org/atomic, golang.org/x/sys, bsm/ginkgo,
  bsm/gomega and google/go-cmp.

  The saving shrinks as you use more of the kit, which is the point — you pay
  for what you load, and nothing else:

  | Sources used | v1.8.0 binary | v2.0.0 binary | Linked packages |
  |---|---|---|---|
  | file | 9,699,758 | 3,952,636 (−59.3%) | 248 → 78 |
  | file + Redis | 13,185,372 | 10,761,881 (−18.4%) | 248 → 200 |
  | file + remote | 11,281,901 | 10,828,383 (−4.0%) | 248 → 229 |
  | all three | 13,185,412 | 13,209,014 (+0.2%) | 248 → 250 |

  A program that really does use all three sources pays 23,602 bytes and two
  packages for the indirection. That is the honest cost of the split, and it
  is the case the old layout charged everybody for.

  Deprecated shims were not an option: a shim for `FromRedis` has to import
  go-redis, which relinks it and gives the entire benefit back. This is the
  same reason health-kit could not keep one when its Redis probe moved out.

  Subpackages are enough; neither dependency needs its own module. Module
  graph pruning keeps a requirement that no imported package needs out of the
  consumer's `go.mod` and `go.sum` entirely — which is what the table above
  measures.

  **The module path is therefore now `github.com/soulteary/parser-kit/v2`**,
  by the import compatibility rule. Every user must update the import path,
  including programs that only ever read files.

- **`LoadOptions` is generic: `LoadOptions[T]`.** `KeyFunc` was
  `interface{}` with a comment explaining that generics could not be used in
  struct fields — they can, on the struct — and a type assertion that turned
  a wrong signature into a run-time error from `NewLoader`. It is now
  `KeyFunc[T]`, so the compiler rejects a mismatch at the call site and
  `NewLoader` has one less way to fail. `DefaultLoadOptions()` becomes
  `DefaultLoadOptions[T]()`.

- **`DataLoader` has two methods instead of four.** `FromFile`, `FromRemote`
  and `FromRedis` collapse into `LoadOne(ctx, Fetcher)`; `Load` is unchanged
  in meaning. An interface with a method per source type could not grow a new
  source without breaking every implementation of it.

- **An empty payload decodes to an empty list rather than a JSON syntax
  error.** A zero-byte file, or a fetcher reporting an absent source, is now
  "no items" instead of "unexpected end of JSON input". With the default
  `AllowEmptyData: false` a fallback chain behaves exactly as before — an
  empty result still moves to the next source — so this is visible only in
  the error text, and when `AllowEmptyData` is set.

### Added

- **`Fetcher`, the extension point.** A source is anything with
  `Fetch(ctx, maxBytes) ([]byte, error)`. Redis, HTTP, a file, an S3 object,
  a Consul key or a test fixture are all the same shape, and adding one no
  longer means adding a `SourceType` constant and a case to a switch inside
  this package.
- **`ReadLimited`**, so a fetcher outside this package enforces the byte
  budget the same way the built-in ones do: read one past the limit, report
  `ErrSourceTooLarge` rather than truncate.
- **`File`, `BytesFetcher`, `At`** in the root package, and
  `ErrNoFetcher` for a `Source` with nothing in it.
- **`redissource`**, with `redissource.Getter` — the two commands the source
  actually issues. `*redis.Client`, `*redis.ClusterClient`, `*redis.Ring` and
  `redis.UniversalClient` all satisfy it, so a cluster or Sentinel deployment
  works where v1's `client.(*redis.Client)` assertion returned "invalid Redis
  client type" at run time.

  Taking an interface reopens a hole that a concrete `*redis.Client` closed by
  construction, so the source closes it explicitly: a plain `c == nil` misses
  a typed nil, such as an unassigned `*redis.Client` field, and calling
  through one panics. `redissource` checks with `reflect` and reports
  `ErrClientNil` instead.
- **`remotesource`**, with per-source `WithAuthorization`, `WithHeader`,
  `WithTimeout`, `WithRetry`, `WithUserAgent`, `WithInsecureSkipVerify` and
  `WithClient`. In v1 one `LoadOptions` set the timeout and retry policy for
  every source in the loader, remote or not, and one HTTP client was built
  per loader whether or not any source was remote. `WithClient` is how
  several remote sources share one connection pool now that each builds its
  own by default.
- **`redissource.New(...).AllowMissing()`**, the Redis counterpart of
  `File(...).AllowMissing()`: a key that was never set stops being the reason
  a whole fallback chain reports failure.
- `CHANGELOG.md`.
- Runnable examples in all three packages (`Example`,
  `ExampleFileFetcher_AllowMissing`, `ExampleLoadStrategyMerge`,
  `ExampleErrSourceTooLarge`, `ExampleFetcher`, `ExampleGetter`,
  `ExampleRetryPolicy`) that `go test` verifies, so they cannot drift from
  the API.
- A package doc in `doc.go` describing the layout and how to write a fetcher.
- `.github/workflows/release.yml`. Nine tags exist with nothing having checked
  any of them, and the mistake this split makes possible — tagging `v2.0.0` on
  a commit whose `go.mod` still says `parser-kit` with no `/v2` — is caught at
  tag time or not at all. It runs on a `v*` tag (and on demand): the module
  path must carry the tag's major version, with v0 and v1 taking no suffix,
  and both READMEs' `go get` line must name that same path. Then the CI gate
  against the tagged commit — gofmt, `go mod tidy` cleanliness, vet,
  golangci-lint, `go test -race` with coverage, and govulncheck. Verification
  only: it publishes nothing and takes no write permissions.
- `.github/dependabot.yml`. Weekly gomod and github-actions updates, minor and
  patch grouped into one PR, majors left separate — for this module a
  dependency major is a judgement call. The release gate is an action too, so
  a silently stale action would be a stale release check.

### Fixed

- **A zero `RetryDelay` no longer means "retry immediately".** v1.7.0 fixed
  this for `MaxRetryDelay` but left the same hole one field over:
  http-kit computes `RetryDelay × multiplier^attempt`, so a zero
  `RetryDelay` stays zero however many times it is doubled, and
  `&LoadOptions{MaxRetries: 3}` — a perfectly ordinary thing to write —
  retried a failing source as fast as the network allowed. `RetryPolicy`
  replaces any non-positive field with its default, and says so in its
  documentation for both fields.
- **An oversized Redis value is caught even if `STRLEN` disagrees with
  `GET`.** The size check and the read are two round trips, and `STRLEN`
  reports 0 for a key holding a non-string type, so the value that actually
  arrived could exceed `MaxBytes`. It is re-checked after `GET`.
- **The loader re-checks the length a fetcher returns.** The byte budget used
  to be enforced entirely inside the three built-in readers; with third-party
  fetchers the budget is only as good as the least careful one, so
  `LoadOne` verifies what came back rather than trusting it.
- **An oversized file is refused without being read.** `os.Stat` already
  reports the size, so a 2 GB file no longer costs a `MaxBytes`-sized read
  before being rejected.
- **`remotesource.New` rejects a URL that is not an absolute http or https
  URL** instead of failing later inside `http.NewRequest`. This is a syntax
  check and not an SSRF defence — see the package documentation, which keeps
  v1's warning.

### Dependencies

- Every direct dependency was already at its latest published version and
  stays there: go-redis v9.22.0, http-kit v1.5.0, miniredis v2.39.0 (test
  only) and testify v1.12.1 (test only). There was no upgrade to take.
- `go.uber.org/atomic` v1.11.0 → v1.12.0 and `yuin/gopher-lua` held at
  v1.1.2, both indirect and both reached only through miniredis, matching the
  other kits.
- Both moved dependencies keep their minimum versions in this module's
  `go.mod`, and MVS still passes those minimums to anyone who imports the
  subpackages. What the split removes is the requirement for everyone else.

[Unreleased]: https://github.com/soulteary/parser-kit/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/soulteary/parser-kit/compare/v1.8.0...v2.0.0
