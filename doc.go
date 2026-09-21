// Package parserkit loads a JSON list from one or more sources, in priority
// order, with either fallback or merge semantics.
//
// # Layout
//
// The root package depends on nothing outside the standard library. Everything
// that needs a third-party client lives in a subpackage, so a program links
// only what it actually loads from:
//
//	parserkit                  the loader, the file source, stdlib only
//	parserkit/redissource      a Redis source            (go-redis)
//	parserkit/remotesource     an HTTP source            (http-kit)
//
// Importing the root package alone costs nothing beyond the standard library.
// A service that reads its rules from a file on disk does not link go-redis or
// http-kit, and neither appears in its go.sum. Since http-kit v2, importing
// parserkit/remotesource no longer links OpenTelemetry either -- trace
// propagation is opt-in, through remotesource.WithPropagator.
//
// # Sources
//
// A source is anything that can produce bytes: a [Fetcher]. The root package
// ships [File]; the subpackages ship the rest, and so can you.
//
//	loader, err := parserkit.NewLoader[Rule](nil)
//	rules, err := loader.Load(ctx,
//		parserkit.At(0, parserkit.File("/etc/app/rules.json")),
//		parserkit.At(1, redissource.New(rdb, "app:rules")),
//	)
//
// Priority is the order the loader tries sources in -- lower first. What it
// does with each result is [LoadStrategy]: LoadStrategyFallback returns the
// first source that yields data, LoadStrategyMerge combines them all and
// deduplicates with LoadOptions.KeyFunc.
//
// # Writing a fetcher
//
// A Fetcher returns the raw bytes of one source and enforces the byte budget
// it is handed. [ReadLimited] does the second part correctly -- it reads one
// byte past the limit so that an oversized source is reported as
// [ErrSourceTooLarge] rather than silently truncated into a JSON syntax error.
// Decoding, normalization, priority and merge semantics are the loader's job,
// not the fetcher's.
package parserkit
