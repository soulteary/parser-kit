package parserkit

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestUser struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Phone string `json:"phone"`
}

// failingFetcher reports an error the way a source that could not be reached
// would.
type failingFetcher struct{ err error }

func (f failingFetcher) Fetch(context.Context, int64) ([]byte, error) { return nil, f.err }

// oversizeFetcher ignores the budget it is handed, the way a careless
// third-party fetcher would.
type oversizeFetcher struct{ n int }

func (o oversizeFetcher) Fetch(context.Context, int64) ([]byte, error) {
	return []byte(strings.Repeat("x", o.n)), nil
}

func newTestLoader(t *testing.T, opts *LoadOptions[TestUser]) DataLoader[TestUser] {
	t.Helper()
	l, err := NewLoader[TestUser](opts)
	require.NoError(t, err)
	return l
}

func TestDefaultLoadOptions(t *testing.T) {
	opts := DefaultLoadOptions[TestUser]()
	assert.Equal(t, DefaultMaxBytes, opts.MaxBytes)
	assert.Equal(t, int64(10*1024*1024), opts.MaxBytes)
	assert.Equal(t, LoadStrategyFallback, opts.LoadStrategy)
	assert.False(t, opts.AllowEmptyData)
	assert.Nil(t, opts.KeyFunc)
}

func TestLoadStrategyConstants(t *testing.T) {
	assert.Equal(t, LoadStrategy("fallback"), LoadStrategyFallback)
	assert.Equal(t, LoadStrategy("merge"), LoadStrategyMerge)
}

func TestNewLoader_NilOptsUsesDefaults(t *testing.T) {
	l := newTestLoader(t, nil).(*loader[TestUser])
	assert.Equal(t, DefaultMaxBytes, l.options.MaxBytes)
	assert.Equal(t, LoadStrategyFallback, l.options.LoadStrategy)
}

func TestNewLoader_OptsDefaulting(t *testing.T) {
	for _, maxBytes := range []int64{0, -1} {
		l := newTestLoader(t, &LoadOptions[TestUser]{MaxBytes: maxBytes}).(*loader[TestUser])
		assert.Equal(t, DefaultMaxBytes, l.options.MaxBytes, "MaxBytes %d", maxBytes)
		assert.Equal(t, LoadStrategyFallback, l.options.LoadStrategy)
	}
}

func TestNewLoader_MergeWithoutKeyFunc(t *testing.T) {
	_, err := NewLoader[TestUser](&LoadOptions[TestUser]{LoadStrategy: LoadStrategyMerge})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KeyFunc is required")
}

func TestNewLoaderWithNormalize_NilOpts(t *testing.T) {
	l, err := NewLoaderWithNormalize[TestUser](nil, func(users []TestUser) []TestUser {
		return users[:0]
	})
	require.NoError(t, err)

	users, err := l.LoadOne(context.Background(), BytesFetcher(`[{"id":"1"}]`))
	require.NoError(t, err)
	assert.Empty(t, users)
}

func TestLoadOne_File(t *testing.T) {
	path := writeFile(t, "users.json", `[{"id":"1","email":"a@example.com"},{"id":"2"}]`)

	users, err := newTestLoader(t, nil).LoadOne(context.Background(), File(path))
	require.NoError(t, err)
	require.Len(t, users, 2)
	assert.Equal(t, "a@example.com", users[0].Email)
}

func TestLoadOne_NilFetcher(t *testing.T) {
	_, err := newTestLoader(t, nil).LoadOne(context.Background(), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoFetcher)
}

func TestLoadOne_EmptyArray(t *testing.T) {
	users, err := newTestLoader(t, nil).LoadOne(context.Background(), BytesFetcher(`[]`))
	require.NoError(t, err)
	assert.NotNil(t, users)
	assert.Empty(t, users)
}

// No bytes means an absent source, not malformed JSON.
func TestLoadOne_NoBytesIsEmpty(t *testing.T) {
	for _, raw := range []BytesFetcher{nil, BytesFetcher("")} {
		users, err := newTestLoader(t, nil).LoadOne(context.Background(), raw)
		require.NoError(t, err)
		assert.NotNil(t, users)
		assert.Empty(t, users)
	}
}

// JSON null decodes to no items rather than a nil slice callers must guard.
func TestLoadOne_JSONNullIsEmpty(t *testing.T) {
	users, err := newTestLoader(t, nil).LoadOne(context.Background(), BytesFetcher(`null`))
	require.NoError(t, err)
	assert.NotNil(t, users)
	assert.Empty(t, users)
}

func TestLoadOne_InvalidJSON(t *testing.T) {
	_, err := newTestLoader(t, nil).LoadOne(context.Background(), BytesFetcher(`{not json`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse JSON")
	assert.NotErrorIs(t, err, ErrSourceTooLarge)
}

func TestLoadOne_PassesBudgetToFetcher(t *testing.T) {
	f := &countingFetcher{payload: `[]`}
	_, err := newTestLoader(t, &LoadOptions[TestUser]{MaxBytes: 4096}).LoadOne(context.Background(), f)
	require.NoError(t, err)
	assert.Equal(t, 1, f.calls)
	assert.Equal(t, int64(4096), f.limit)
}

// The budget is only as good as the least careful Fetcher, so the loader
// re-checks what came back instead of trusting it.
func TestLoadOne_FetcherIgnoringBudgetIsCaught(t *testing.T) {
	_, err := newTestLoader(t, &LoadOptions[TestUser]{MaxBytes: 4}).
		LoadOne(context.Background(), oversizeFetcher{n: 64})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSourceTooLarge)
	assert.Contains(t, err.Error(), "fetcher returned 64 bytes")
}

func TestLoadOne_FetcherErrorIsReturnedUnwrapped(t *testing.T) {
	sentinel := errors.New("source unreachable")
	_, err := newTestLoader(t, nil).LoadOne(context.Background(), failingFetcher{err: sentinel})
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
}

func TestLoader_WithNormalizeFunc(t *testing.T) {
	normalize := func(users []TestUser) []TestUser {
		for i := range users {
			users[i].Email = strings.ToLower(users[i].Email)
		}
		return users
	}
	l, err := NewLoaderWithNormalize[TestUser](nil, normalize)
	require.NoError(t, err)

	users, err := l.LoadOne(context.Background(), BytesFetcher(`[{"id":"1","email":"A@EXAMPLE.COM"}]`))
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "a@example.com", users[0].Email)
}

func TestLoad_NoSources(t *testing.T) {
	_, err := newTestLoader(t, nil).Load(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no sources provided")
}

func TestLoad_Priority(t *testing.T) {
	users, err := newTestLoader(t, nil).Load(context.Background(),
		At(1, BytesFetcher(`[{"id":"low"}]`)),
		At(0, BytesFetcher(`[{"id":"high"}]`)),
	)
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "high", users[0].ID)
}

func TestLoad_Fallback(t *testing.T) {
	users, err := newTestLoader(t, nil).Load(context.Background(),
		At(0, File(t.TempDir()+"/absent.json")),
		At(1, BytesFetcher(`[{"id":"backup"}]`)),
	)
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "backup", users[0].ID)
}

// An empty-but-successful source is a failure unless AllowEmptyData says so.
func TestLoad_EmptySourceFallsThrough(t *testing.T) {
	users, err := newTestLoader(t, nil).Load(context.Background(),
		At(0, BytesFetcher(`[]`)),
		At(1, BytesFetcher(`[{"id":"backup"}]`)),
	)
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "backup", users[0].ID)
}

func TestLoad_AllowEmptyData(t *testing.T) {
	users, err := newTestLoader(t, &LoadOptions[TestUser]{AllowEmptyData: true}).Load(context.Background(),
		At(0, BytesFetcher(`[]`)),
		At(1, BytesFetcher(`[{"id":"backup"}]`)),
	)
	require.NoError(t, err)
	assert.Empty(t, users)
}

func TestLoad_AllSourcesFail(t *testing.T) {
	_, err := newTestLoader(t, nil).Load(context.Background(),
		At(0, failingFetcher{err: errors.New("first down")}),
		At(1, failingFetcher{err: errors.New("second down")}),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "all sources failed")
	assert.Contains(t, err.Error(), "second down")
}

func TestLoad_AllSourcesEmpty(t *testing.T) {
	_, err := newTestLoader(t, nil).Load(context.Background(), At(0, BytesFetcher(`[]`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "source returned empty data")
}

func TestLoad_NilFetcherInSource(t *testing.T) {
	_, err := newTestLoader(t, nil).Load(context.Background(), Source{Priority: 0})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoFetcher)
}

func TestLoad_StrategyEmptyUsesFallback(t *testing.T) {
	l := &loader[TestUser]{options: &LoadOptions[TestUser]{MaxBytes: DefaultMaxBytes}}
	users, err := l.Load(context.Background(), At(0, BytesFetcher(`[{"id":"1"}]`)))
	require.NoError(t, err)
	assert.Len(t, users, 1)
}

func mergeOptions() *LoadOptions[TestUser] {
	return &LoadOptions[TestUser]{
		LoadStrategy: LoadStrategyMerge,
		KeyFunc: func(u TestUser) (string, bool) {
			if u.ID == "" {
				return "", false
			}
			return u.ID, true
		},
	}
}

func TestLoad_Merge(t *testing.T) {
	users, err := newTestLoader(t, mergeOptions()).Load(context.Background(),
		At(0, BytesFetcher(`[{"id":"1","email":"first"},{"id":"2"}]`)),
		At(1, BytesFetcher(`[{"id":"2","email":"second"},{"id":"3"}]`)),
	)
	require.NoError(t, err)
	require.Len(t, users, 3)
	assert.Equal(t, []string{"1", "2", "3"}, []string{users[0].ID, users[1].ID, users[2].ID})
	// A later source overwrites the value but not the position.
	assert.Equal(t, "second", users[1].Email)
}

func TestLoad_Merge_KeyFuncExcludes(t *testing.T) {
	users, err := newTestLoader(t, mergeOptions()).Load(context.Background(),
		At(0, BytesFetcher(`[{"id":"1"},{"id":""},{"id":"2"}]`)),
	)
	require.NoError(t, err)
	require.Len(t, users, 2)
}

func TestLoad_Merge_DuplicateKeyInOneSource(t *testing.T) {
	users, err := newTestLoader(t, mergeOptions()).Load(context.Background(),
		At(0, BytesFetcher(`[{"id":"1","email":"a"},{"id":"1","email":"b"}]`)),
	)
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "b", users[0].Email)
}

func TestLoad_Merge_AllSourcesFail(t *testing.T) {
	_, err := newTestLoader(t, mergeOptions()).Load(context.Background(),
		At(0, failingFetcher{err: errors.New("down")}),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "all sources failed")
}

func TestLoad_Merge_AllEmpty(t *testing.T) {
	_, err := newTestLoader(t, mergeOptions()).Load(context.Background(),
		At(0, BytesFetcher(`[]`)),
		At(1, BytesFetcher(`[]`)),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "all sources returned empty data")
}

func TestLoad_Merge_AllEmpty_AllowEmptyData(t *testing.T) {
	opts := mergeOptions()
	opts.AllowEmptyData = true

	users, err := newTestLoader(t, opts).Load(context.Background(),
		At(0, BytesFetcher(`[]`)),
		At(1, BytesFetcher(`[]`)),
	)
	require.NoError(t, err)
	assert.Empty(t, users)
}

// Reaching loadWithMerge without a KeyFunc is impossible through NewLoader,
// which rejects it; the guard inside still has to hold.
func TestLoad_Merge_NilKeyFuncGuard(t *testing.T) {
	l := &loader[TestUser]{options: &LoadOptions[TestUser]{
		MaxBytes:     DefaultMaxBytes,
		LoadStrategy: LoadStrategyMerge,
	}}
	_, err := l.Load(context.Background(), At(0, BytesFetcher(`[{"id":"1"}]`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KeyFunc is required")
}
