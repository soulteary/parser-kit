package parserkit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
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
