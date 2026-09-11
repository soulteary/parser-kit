package parserkit

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestOversizedSourceIsDistinguishable: io.LimitReader truncates silently, so
// an oversized file used to surface as a JSON parse error with no way to tell
// "too large" from "malformed".
func TestOversizedSourceIsDistinguishable(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "big-*.json")
	if err != nil {
		t.Fatal(err)
	}
	// Valid JSON, just over the limit we will configure.
	payload := `[{"id":"1","email":"` + strings.Repeat("a", 300) + `@b.com","phone":"1"}]`
	if _, err := f.WriteString(payload); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	loader, err := NewLoader[TestUser](&LoadOptions{MaxFileSize: 64})
	if err != nil {
		t.Fatal(err)
	}

	_, err = loader.FromFile(context.Background(), f.Name())
	if err == nil {
		t.Fatal("FromFile on an oversized file returned nil error")
	}
	if !errors.Is(err, ErrSourceTooLarge) {
		t.Errorf("FromFile error = %v, want it to wrap ErrSourceTooLarge rather than look like malformed JSON", err)
	}

	// A file inside the limit still loads.
	small, err := os.CreateTemp(t.TempDir(), "small-*.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := small.WriteString(`[{"id":"1","email":"a@b.com","phone":"1"}]`); err != nil {
		t.Fatal(err)
	}
	_ = small.Close()

	users, err := loader.FromFile(context.Background(), small.Name())
	if err != nil {
		t.Fatalf("FromFile on a small file error = %v", err)
	}
	if len(users) != 1 {
		t.Errorf("loaded %d users, want 1", len(users))
	}
}

func TestOversizedRemoteIsDistinguishable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"1","email":"` + strings.Repeat("a", 300) + `@b.com","phone":"1"}]`))
	}))
	defer srv.Close()

	loader, err := NewLoader[TestUser](&LoadOptions{MaxFileSize: 64})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := loader.FromRemote(context.Background(), srv.URL, ""); !errors.Is(err, ErrSourceTooLarge) {
		t.Errorf("FromRemote error = %v, want it to wrap ErrSourceTooLarge", err)
	}
}

// TestSourceOrderIsStable: sort.Slice is not stable, so two sources sharing a
// priority took an arbitrary order that could differ between runs.
func TestSourceOrderIsStable(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		path := dir + "/" + name
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}

	first := write("a.json", `[{"id":"first","email":"a@b.com","phone":"1"}]`)
	second := write("b.json", `[{"id":"second","email":"c@d.com","phone":"2"}]`)

	loader, err := NewLoader[TestUser](nil)
	if err != nil {
		t.Fatal(err)
	}

	// Both sources share priority 1; the first listed must always win.
	for i := 0; i < 50; i++ {
		users, err := loader.Load(context.Background(),
			Source{Type: SourceTypeFile, Priority: 1, Config: SourceConfig{FilePath: first}},
			Source{Type: SourceTypeFile, Priority: 1, Config: SourceConfig{FilePath: second}},
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(users) == 0 || users[0].ID != "first" {
			t.Fatalf("iteration %d returned %v; equal priorities are not resolved stably", i, users)
		}
	}
}

// TestNewLoaderDoesNotMutateOptions: defaults were written back into the
// caller's struct, which surprises anyone reusing or sharing it.
func TestNewLoaderDoesNotMutateOptions(t *testing.T) {
	opts := &LoadOptions{}
	if _, err := NewLoader[TestUser](opts); err != nil {
		t.Fatal(err)
	}
	if opts.MaxFileSize != 0 || opts.LoadStrategy != "" {
		t.Errorf("NewLoader mutated the caller's options: %+v", opts)
	}
}

// --- Codex review follow-ups (PR #3) ---

// TestMaxFileSizeMaxInt64DoesNotOverflow is the regression test for the
// one-byte overrun margin: MaxFileSize + 1 wraps to math.MinInt64 when a
// caller passes math.MaxInt64 to mean "no limit", and io.LimitReader with a
// negative budget returns EOF immediately, so every file -- however small --
// came back as malformed JSON.
func TestMaxFileSizeMaxInt64DoesNotOverflow(t *testing.T) {
	if got := readLimit(math.MaxInt64); got != math.MaxInt64 {
		t.Errorf("readLimit(MaxInt64) = %d, want MaxInt64 (it must saturate, not wrap)", got)
	}
	if got := readLimit(64); got != 65 {
		t.Errorf("readLimit(64) = %d, want 65", got)
	}

	f, err := os.CreateTemp(t.TempDir(), "unlimited-*.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`[{"id":"1","email":"a@b.com","phone":"1"}]`); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	loader, err := NewLoader[TestUser](&LoadOptions{MaxFileSize: math.MaxInt64})
	if err != nil {
		t.Fatal(err)
	}

	users, err := loader.FromFile(context.Background(), f.Name())
	if err != nil {
		t.Fatalf("FromFile with MaxFileSize=MaxInt64 error = %v, want the file to load", err)
	}
	if len(users) != 1 {
		t.Errorf("loaded %d users, want 1", len(users))
	}

	// Same overflow on the remote path.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"2","email":"c@d.com","phone":"2"}]`))
	}))
	defer srv.Close()

	users, err = loader.FromRemote(context.Background(), srv.URL, "")
	if err != nil {
		t.Fatalf("FromRemote with MaxFileSize=MaxInt64 error = %v, want the body to load", err)
	}
	if len(users) != 1 {
		t.Errorf("loaded %d users from remote, want 1", len(users))
	}
}

// TestRedisOversizeWrapsSentinel: the Redis path reported an oversized value
// with a plain formatted error, so errors.Is(err, ErrSourceTooLarge) was false
// and callers could not classify the condition the way they can for the file
// and remote paths.
func TestRedisOversizeWrapsSentinel(t *testing.T) {
	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	defer func() { _ = client.Close() }()

	if err := client.Set(context.Background(), "users", strings.Repeat("x", 512), 0).Err(); err != nil {
		t.Fatal(err)
	}

	loader, err := NewLoader[TestUser](&LoadOptions{MaxFileSize: 64})
	if err != nil {
		t.Fatal(err)
	}

	_, err = loader.FromRedis(context.Background(), client, "users")
	if err == nil {
		t.Fatal("FromRedis on an oversized value returned nil error")
	}
	if !errors.Is(err, ErrSourceTooLarge) {
		t.Errorf("FromRedis error = %v, want it to wrap ErrSourceTooLarge like the file and remote paths", err)
	}
}
