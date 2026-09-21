package parserkit

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An oversized source must be distinguishable from a malformed one.
//
// io.LimitReader truncates silently, so before v1.6.0 a file larger than the
// limit came back as "failed to parse JSON" -- the same error as genuinely
// broken content, with no way to tell which had happened.
func TestOversizedSourceIsDistinguishable(t *testing.T) {
	path := writeFile(t, "users.json", `[{"id":"1","email":"user@example.com"}]`)

	l := newTestLoader(t, &LoadOptions[TestUser]{MaxBytes: 8})
	_, err := l.LoadOne(context.Background(), File(path))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSourceTooLarge)
	assert.NotContains(t, err.Error(), "failed to parse JSON")
}

// Sources sharing a priority must keep the order they were passed in.
// sort.Slice is not stable, so identical input could order differently
// between runs.
func TestSourceOrderIsStable(t *testing.T) {
	l := newTestLoader(t, nil)

	sources := []Source{
		At(0, failingFetcher{err: assertError("first")}),
		At(0, BytesFetcher(`[{"id":"second"}]`)),
		At(0, BytesFetcher(`[{"id":"third"}]`)),
	}

	for range 50 {
		users, err := l.Load(context.Background(), sources...)
		require.NoError(t, err)
		require.Len(t, users, 1)
		require.Equal(t, "second", users[0].ID)
	}
}

type assertError string

func (e assertError) Error() string { return string(e) }

// NewLoader must not write its defaults back into the caller's struct.
func TestNewLoaderDoesNotMutateOptions(t *testing.T) {
	opts := &LoadOptions[TestUser]{}
	_, err := NewLoader[TestUser](opts)
	require.NoError(t, err)

	assert.Zero(t, opts.MaxBytes, "MaxBytes was written back into the caller's struct")
	assert.Empty(t, opts.LoadStrategy, "LoadStrategy was written back into the caller's struct")
}

// MaxBytes: math.MaxInt64 is the natural way to say "no limit". The one-byte
// overrun margin used to wrap to math.MinInt64, so io.LimitReader returned EOF
// immediately and every source -- however small -- came back as malformed JSON.
func TestMaxBytesMaxInt64DoesNotOverflow(t *testing.T) {
	path := writeFile(t, "users.json", `[{"id":"1","email":"user@example.com"}]`)

	l := newTestLoader(t, &LoadOptions[TestUser]{MaxBytes: math.MaxInt64})

	users, err := l.LoadOne(context.Background(), File(path))
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "user@example.com", users[0].Email)

	// And through Load, with an in-memory source too.
	users, err = l.Load(context.Background(), At(0, BytesFetcher(`[{"id":"2"}]`)))
	require.NoError(t, err)
	require.Len(t, users, 1)
}

// The read limit is a budget, not a target: a source at exactly the limit is
// fine, one byte over is not.
func TestLimitBoundaryIsExact(t *testing.T) {
	payload := `[{"id":"1"}]`
	size := int64(len(payload))

	l := newTestLoader(t, &LoadOptions[TestUser]{MaxBytes: size})
	users, err := l.LoadOne(context.Background(), BytesFetcher(payload))
	require.NoError(t, err)
	require.Len(t, users, 1)

	l = newTestLoader(t, &LoadOptions[TestUser]{MaxBytes: size - 1})
	_, err = l.LoadOne(context.Background(), BytesFetcher(payload))
	assert.ErrorIs(t, err, ErrSourceTooLarge)
}

// A source that is under the limit but whose content is broken still reports
// as broken, not as too large.
func TestMalformedStaysMalformed(t *testing.T) {
	l := newTestLoader(t, &LoadOptions[TestUser]{MaxBytes: 1024})
	_, err := l.LoadOne(context.Background(), BytesFetcher(strings.Repeat("[", 4)))
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrSourceTooLarge)
	assert.Contains(t, err.Error(), "failed to parse JSON")
}
