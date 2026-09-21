package parserkit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

// DefaultMaxBytes is the byte budget a source gets when LoadOptions.MaxBytes
// is left unset.
const DefaultMaxBytes int64 = 10 * 1024 * 1024 // 10MB

// Fetcher retrieves the raw, still-encoded bytes of a single source.
//
// It is the whole extension point: a Redis key, an HTTP endpoint, an S3
// object or a test fixture are all just Fetchers, and the loader does not
// need to know which. The root package ships File; parserkit/redissource and
// parserkit/remotesource ship the two that need a third-party client.
//
// maxBytes is LoadOptions.MaxBytes. An implementation must not return more
// than that, and must report an overrun as an error wrapping
// ErrSourceTooLarge rather than truncating -- ReadLimited does both. The
// loader re-checks the length it gets back, so a fetcher that ignores the
// budget fails loudly instead of quietly.
//
// Returning no bytes and no error means the source is absent -- a file that
// does not exist, a key that was never set -- and the loader reads that as an
// empty result, which LoadOptions.AllowEmptyData then governs.
type Fetcher interface {
	Fetch(ctx context.Context, maxBytes int64) ([]byte, error)
}

// Source is a Fetcher with a priority attached.
type Source struct {
	// Fetcher retrieves the bytes.
	Fetcher Fetcher

	// Priority orders the source against the others in one Load call.
	// Lower number = higher priority (0 is highest). Sources sharing a
	// priority keep the order they were passed in.
	Priority int
}

// At pairs a fetcher with a priority.
//
//	loader.Load(ctx,
//		parserkit.At(0, parserkit.File("/etc/app/rules.json")),
//		parserkit.At(1, redissource.New(rdb, "app:rules")),
//	)
func At(priority int, fetcher Fetcher) Source {
	return Source{Fetcher: fetcher, Priority: priority}
}

// ReadLimited reads r into memory, refusing to return more than maxBytes.
//
// It reads one byte past the limit, because io.LimitReader truncates
// silently: without the extra byte an oversized source comes back as a JSON
// syntax error, indistinguishable from genuinely malformed content. An
// overrun is reported as an error wrapping ErrSourceTooLarge.
//
// maxBytes of zero or less means DefaultMaxBytes, so a Fetcher used on its
// own still has a bound. Callers should add their own context:
//
//	raw, err := parserkit.ReadLimited(resp.Body, maxBytes)
//	if err != nil {
//		return nil, fmt.Errorf("response from %q: %w", url, err)
//	}
func ReadLimited(r io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	raw, err := io.ReadAll(io.LimitReader(r, readLimit(maxBytes)))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maxBytes {
		return nil, fmt.Errorf("%w (limit %d bytes)", ErrSourceTooLarge, maxBytes)
	}
	return raw, nil
}

// readLimit is the byte budget handed to io.LimitReader: one past maxBytes,
// so reading the extra byte makes an overrun detectable rather than silently
// truncating. It saturates instead of wrapping, because MaxBytes set to
// math.MaxInt64 -- the natural way to say "no limit" -- would otherwise
// overflow to math.MinInt64 and make LimitReader return EOF immediately.
func readLimit(maxBytes int64) int64 {
	// == rather than >=: for an int64 the two are equivalent here, and
	// staticcheck rightly points out that nothing exceeds math.MaxInt64.
	if maxBytes == math.MaxInt64 {
		return math.MaxInt64
	}
	return maxBytes + 1
}

// FileFetcher reads a source from the local filesystem. Use File to build one.
type FileFetcher struct {
	path         string
	allowMissing bool
}

// File returns a Fetcher that reads path.
func File(path string) *FileFetcher {
	return &FileFetcher{path: path}
}

// AllowMissing makes a file that does not exist report an absent source
// rather than an error, so the loader moves on to the next source without
// the missing file showing up as the reason everything failed.
//
// It says nothing about a file that exists and is unreadable: a permission
// error is still an error.
func (f *FileFetcher) AllowMissing() *FileFetcher {
	f.allowMissing = true
	return f
}

// Path returns the path this fetcher reads.
func (f *FileFetcher) Path() string {
	return f.path
}

// Fetch reads the file, subject to maxBytes.
func (f *FileFetcher) Fetch(_ context.Context, maxBytes int64) ([]byte, error) {
	if f.path == "" {
		return nil, errors.New("parserkit: file path not specified")
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}

	info, err := os.Stat(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			if f.allowMissing {
				return nil, nil // absent, not a failure
			}
			return nil, fmt.Errorf("file not found: %s", f.path)
		}
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	// Refuse an oversized file before reading it. Only for regular files:
	// Size() means nothing for a directory or a device node, and those fail
	// on the read below with an error that says what actually went wrong.
	if info.Mode().IsRegular() && info.Size() > maxBytes {
		return nil, fmt.Errorf("file %q: %w (limit %d bytes)", f.path, ErrSourceTooLarge, maxBytes)
	}

	file, err := os.Open(f.path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	raw, err := ReadLimited(file, maxBytes)
	if err != nil {
		if errors.Is(err, ErrSourceTooLarge) {
			return nil, fmt.Errorf("file %q: %w", f.path, err)
		}
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return raw, nil
}

// BytesFetcher serves a payload already in memory. It is what tests and
// callers that fetched the bytes themselves reach for.
type BytesFetcher []byte

// Fetch returns the bytes, subject to maxBytes.
func (b BytesFetcher) Fetch(_ context.Context, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	if int64(len(b)) > maxBytes {
		return nil, fmt.Errorf("%w (limit %d bytes)", ErrSourceTooLarge, maxBytes)
	}
	return b, nil
}
