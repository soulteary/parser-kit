package parserkit

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestFile_Fetch(t *testing.T) {
	path := writeFile(t, "users.json", `[{"id":"1"}]`)

	raw, err := File(path).Fetch(context.Background(), DefaultMaxBytes)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"id":"1"}]`, string(raw))
}

func TestFile_Path(t *testing.T) {
	assert.Equal(t, "/etc/app/rules.json", File("/etc/app/rules.json").Path())
}

func TestFile_EmptyPath(t *testing.T) {
	_, err := File("").Fetch(context.Background(), DefaultMaxBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "file path not specified")
}

func TestFile_NotFound(t *testing.T) {
	_, err := File(filepath.Join(t.TempDir(), "absent.json")).Fetch(context.Background(), DefaultMaxBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "file not found")
}

func TestFile_NotFound_AllowMissing(t *testing.T) {
	raw, err := File(filepath.Join(t.TempDir(), "absent.json")).AllowMissing().Fetch(context.Background(), DefaultMaxBytes)
	require.NoError(t, err)
	assert.Empty(t, raw)
}

func TestFile_StatError(t *testing.T) {
	// A NUL byte in the path makes Stat fail with something that is not
	// IsNotExist on most systems.
	_, err := File("\x00invalid").Fetch(context.Background(), DefaultMaxBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to stat file")
}

// A directory is not a missing file, so AllowMissing must not swallow it.
func TestFile_PathIsDir(t *testing.T) {
	dir := t.TempDir()

	for _, f := range []*FileFetcher{File(dir), File(dir).AllowMissing()} {
		_, err := f.Fetch(context.Background(), DefaultMaxBytes)
		require.Error(t, err)
		msg := err.Error()
		assert.True(t,
			strings.Contains(msg, "failed to open file") || strings.Contains(msg, "failed to read file"),
			"unexpected error: %s", msg)
	}
}

func TestFile_TooLarge(t *testing.T) {
	path := writeFile(t, "big.json", strings.Repeat("x", 64))

	_, err := File(path).Fetch(context.Background(), 8)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSourceTooLarge)
	assert.Contains(t, err.Error(), "big.json")
}

// Exactly at the limit is not over it.
func TestFile_ExactlyAtLimit(t *testing.T) {
	path := writeFile(t, "exact.json", "[]")

	raw, err := File(path).Fetch(context.Background(), 2)
	require.NoError(t, err)
	assert.Equal(t, "[]", string(raw))
}

func TestFile_NonPositiveLimitUsesDefault(t *testing.T) {
	path := writeFile(t, "users.json", `[{"id":"1"}]`)

	for _, limit := range []int64{0, -1} {
		raw, err := File(path).Fetch(context.Background(), limit)
		require.NoError(t, err, "limit %d", limit)
		assert.JSONEq(t, `[{"id":"1"}]`, string(raw))
	}
}

func TestReadLimited(t *testing.T) {
	raw, err := ReadLimited(strings.NewReader("hello"), 5)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(raw))

	_, err = ReadLimited(strings.NewReader("hello!"), 5)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSourceTooLarge)
}

func TestReadLimited_NonPositiveLimitUsesDefault(t *testing.T) {
	raw, err := ReadLimited(strings.NewReader("hello"), 0)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(raw))
}

// MaxInt64 is the natural way to say "no limit"; the +1 margin must not
// overflow it into MinInt64, which would make LimitReader return EOF at once.
func TestReadLimited_MaxInt64DoesNotOverflow(t *testing.T) {
	raw, err := ReadLimited(strings.NewReader(`[{"id":"1"}]`), math.MaxInt64)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"id":"1"}]`, string(raw))
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("boom") }

func TestReadLimited_ReadError(t *testing.T) {
	_, err := ReadLimited(errReader{}, DefaultMaxBytes)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrSourceTooLarge)
	assert.Contains(t, err.Error(), "boom")
}

func TestBytesFetcher(t *testing.T) {
	raw, err := BytesFetcher(`[{"id":"1"}]`).Fetch(context.Background(), DefaultMaxBytes)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"id":"1"}]`, string(raw))

	_, err = BytesFetcher("0123456789").Fetch(context.Background(), 4)
	assert.ErrorIs(t, err, ErrSourceTooLarge)

	raw, err = BytesFetcher("012").Fetch(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, "012", string(raw))
}

func TestAt(t *testing.T) {
	f := File("a.json")
	src := At(3, f)
	assert.Equal(t, 3, src.Priority)
	assert.Same(t, f, src.Fetcher)
}

// Fetcher is the whole extension point, so a type outside this package must
// be usable without anything else being implemented.
type countingFetcher struct {
	payload string
	calls   int
	limit   int64
}

func (c *countingFetcher) Fetch(_ context.Context, maxBytes int64) ([]byte, error) {
	c.calls++
	c.limit = maxBytes
	return []byte(c.payload), nil
}

var _ Fetcher = (*countingFetcher)(nil)
var _ Fetcher = (*FileFetcher)(nil)
var _ Fetcher = BytesFetcher(nil)
var _ io.Reader = errReader{}
