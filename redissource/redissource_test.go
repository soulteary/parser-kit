package redissource_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	parserkit "github.com/soulteary/parser-kit/v2"
	"github.com/soulteary/parser-kit/v2/redissource"
)

type TestUser struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

func newRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

func newLoader(t *testing.T) parserkit.DataLoader[TestUser] {
	t.Helper()
	l, err := parserkit.NewLoader[TestUser](nil)
	require.NoError(t, err)
	return l
}

// The whole point of Getter: a source written against it works on every
// go-redis client shape, not just *redis.Client. v1 type-asserted to
// *redis.Client and gave up on the rest at run time.
var (
	_ redissource.Getter = (*redis.Client)(nil)
	_ redissource.Getter = (*redis.ClusterClient)(nil)
	_ redissource.Getter = (*redis.Ring)(nil)
	_ redissource.Getter = (redis.UniversalClient)(nil)
)

func TestGetter_AcceptsEveryClientShape(t *testing.T) {
	mr, _ := newRedis(t)
	require.NoError(t, mr.Set("users", `[{"id":"1"}]`))

	clients := map[string]redissource.Getter{
		"client": redis.NewClient(&redis.Options{Addr: mr.Addr()}),
		"ring":   redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"one": mr.Addr()}}),
		"universal": redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs: []string{mr.Addr()},
		}),
	}

	for name, client := range clients {
		t.Run(name, func(t *testing.T) {
			raw, err := redissource.New(client, "users").Fetch(context.Background(), parserkit.DefaultMaxBytes)
			require.NoError(t, err)
			assert.JSONEq(t, `[{"id":"1"}]`, string(raw))
		})
	}
}

func TestFetch_Success(t *testing.T) {
	mr, client := newRedis(t)
	require.NoError(t, mr.Set("users", `[{"id":"1","email":"a@example.com"},{"id":"2"}]`))

	users, err := newLoader(t).LoadOne(context.Background(), redissource.New(client, "users"))
	require.NoError(t, err)
	require.Len(t, users, 2)
	assert.Equal(t, "a@example.com", users[0].Email)
}

func TestFetch_ThroughUniversalClient(t *testing.T) {
	mr, _ := newRedis(t)
	universal := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{mr.Addr()}})
	t.Cleanup(func() { _ = universal.Close() })
	require.NoError(t, mr.Set("users", `[{"id":"1"}]`))

	users, err := newLoader(t).LoadOne(context.Background(), redissource.New(universal, "users"))
	require.NoError(t, err)
	assert.Len(t, users, 1)
}

func TestFetch_Key(t *testing.T) {
	assert.Equal(t, "app:rules", redissource.New(nil, "app:rules").Key())
}

func TestFetch_KeyNotFound(t *testing.T) {
	_, client := newRedis(t)

	_, err := redissource.New(client, "absent").Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key not found in Redis")
}

func TestFetch_KeyNotFound_AllowMissing(t *testing.T) {
	_, client := newRedis(t)

	raw, err := redissource.New(client, "absent").AllowMissing().Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.NoError(t, err)
	assert.Empty(t, raw)

	// And through the loader: an absent source is an empty result, so the
	// next source gets its turn.
	users, err := newLoader(t).Load(context.Background(),
		parserkit.At(0, redissource.New(client, "absent").AllowMissing()),
		parserkit.At(1, parserkit.BytesFetcher(`[{"id":"backup"}]`)),
	)
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "backup", users[0].ID)
}

func TestFetch_EmptyKey(t *testing.T) {
	_, client := newRedis(t)

	_, err := redissource.New(client, "").Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redis key not specified")
}

func TestFetch_NilClient(t *testing.T) {
	_, err := redissource.New(nil, "users").Fetch(context.Background(), parserkit.DefaultMaxBytes)
	assert.ErrorIs(t, err, redissource.ErrClientNil)
}

// A nil *redis.Client stored in the interface is not a nil interface, and
// calling through it panics. A source that takes the process down is worse
// than the outage it was meant to survive.
func TestFetch_TypedNilClient(t *testing.T) {
	var client *redis.Client // never assigned

	assert.NotPanics(t, func() {
		_, err := redissource.New(client, "users").Fetch(context.Background(), parserkit.DefaultMaxBytes)
		assert.ErrorIs(t, err, redissource.ErrClientNil)
	})
}

func TestFetch_InvalidJSON(t *testing.T) {
	mr, client := newRedis(t)
	require.NoError(t, mr.Set("users", "{not json"))

	_, err := newLoader(t).LoadOne(context.Background(), redissource.New(client, "users"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse JSON")
	assert.NotErrorIs(t, err, parserkit.ErrSourceTooLarge)
}

// STRLEN refuses an oversized value before it is pulled into memory, and the
// sentinel is wrapped so errors.Is finds it.
func TestFetch_ValueTooLarge(t *testing.T) {
	mr, client := newRedis(t)
	require.NoError(t, mr.Set("users", strings.Repeat("x", 64)))

	_, err := redissource.New(client, "users").Fetch(context.Background(), 8)
	require.Error(t, err)
	assert.ErrorIs(t, err, parserkit.ErrSourceTooLarge)
	assert.Contains(t, err.Error(), `redis key "users" is 64 bytes`)
}

func TestFetch_ExactlyAtLimit(t *testing.T) {
	mr, client := newRedis(t)
	require.NoError(t, mr.Set("users", "[]"))

	raw, err := redissource.New(client, "users").Fetch(context.Background(), 2)
	require.NoError(t, err)
	assert.Equal(t, "[]", string(raw))
}

func TestFetch_NonPositiveLimitUsesDefault(t *testing.T) {
	mr, client := newRedis(t)
	require.NoError(t, mr.Set("users", `[{"id":"1"}]`))

	raw, err := redissource.New(client, "users").Fetch(context.Background(), 0)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"id":"1"}]`, string(raw))
}

func TestFetch_ConnectionError(t *testing.T) {
	mr, client := newRedis(t)
	require.NoError(t, mr.Set("users", `[{"id":"1"}]`))
	mr.Close()

	_, err := redissource.New(client, "users").Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get Redis value size")
}

func TestFetch_ContextCancelled(t *testing.T) {
	_, client := newRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := redissource.New(client, "users").Fetch(ctx, parserkit.DefaultMaxBytes)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// stubGetter drives the paths a real server will not produce on demand.
type stubGetter struct {
	strLen    int64
	strLenErr error
	val       string
	getErr    error
}

func (s stubGetter) StrLen(ctx context.Context, _ string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	if s.strLenErr != nil {
		cmd.SetErr(s.strLenErr)
	} else {
		cmd.SetVal(s.strLen)
	}
	return cmd
}

func (s stubGetter) Get(ctx context.Context, _ string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	if s.getErr != nil {
		cmd.SetErr(s.getErr)
	} else {
		cmd.SetVal(s.val)
	}
	return cmd
}

// STRLEN reporting redis.Nil for a key that GET then finds is not an error:
// it is the key appearing between the two round trips.
func TestFetch_StrLenNilIsNotAnError(t *testing.T) {
	f := redissource.New(stubGetter{strLenErr: redis.Nil, val: `[{"id":"1"}]`}, "users")

	raw, err := f.Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"id":"1"}]`, string(raw))
}

// STRLEN and GET are two round trips, and STRLEN reports 0 for a key holding
// a non-string type. Whatever GET actually returned is re-checked.
func TestFetch_ValueGrownBetweenStrLenAndGet(t *testing.T) {
	f := redissource.New(stubGetter{strLen: 2, val: strings.Repeat("x", 64)}, "users")

	_, err := f.Fetch(context.Background(), 8)
	require.Error(t, err)
	assert.ErrorIs(t, err, parserkit.ErrSourceTooLarge)
	assert.Contains(t, err.Error(), "is 64 bytes")
}

func TestFetch_GetError(t *testing.T) {
	f := redissource.New(stubGetter{strLen: 2, getErr: errors.New("connection reset")}, "users")

	_, err := f.Fetch(context.Background(), parserkit.DefaultMaxBytes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get from Redis")
}

var _ redissource.Getter = stubGetter{}
