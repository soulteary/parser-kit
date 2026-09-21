package parserkit_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	parserkit "github.com/soulteary/parser-kit/v2"
)

type Rule struct {
	ID     string `json:"id"`
	Action string `json:"action"`
}

// Load reads from the highest-priority source that yields data.
func Example() {
	dir, _ := os.MkdirTemp("", "parserkit-example-*")
	defer func() { _ = os.RemoveAll(dir) }()
	path := filepath.Join(dir, "rules.json")
	_ = os.WriteFile(path, []byte(`[{"id":"r1","action":"allow"}]`), 0o600)

	loader, err := parserkit.NewLoader[Rule](nil)
	if err != nil {
		panic(err)
	}

	rules, err := loader.Load(context.Background(),
		parserkit.At(0, parserkit.File(path)),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(len(rules), rules[0].ID, rules[0].Action)
	// Output: 1 r1 allow
}

// A missing primary source falls through to the next one instead of failing
// the whole load.
func ExampleFileFetcher_AllowMissing() {
	loader, err := parserkit.NewLoader[Rule](nil)
	if err != nil {
		panic(err)
	}

	rules, err := loader.Load(context.Background(),
		parserkit.At(0, parserkit.File("/nonexistent/rules.json").AllowMissing()),
		parserkit.At(1, parserkit.BytesFetcher(`[{"id":"builtin","action":"deny"}]`)),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(rules[0].ID, rules[0].Action)
	// Output: builtin deny
}

// LoadStrategyMerge combines every source that succeeded, deduplicating on
// KeyFunc. The first source to define a key fixes its position; a later one
// replaces its value.
func ExampleLoadStrategyMerge() {
	loader, err := parserkit.NewLoader[Rule](&parserkit.LoadOptions[Rule]{
		LoadStrategy: parserkit.LoadStrategyMerge,
		KeyFunc: func(r Rule) (string, bool) {
			return r.ID, r.ID != ""
		},
	})
	if err != nil {
		panic(err)
	}

	rules, err := loader.Load(context.Background(),
		parserkit.At(0, parserkit.BytesFetcher(`[{"id":"r1","action":"allow"}]`)),
		parserkit.At(1, parserkit.BytesFetcher(`[{"id":"r1","action":"deny"},{"id":"r2","action":"allow"}]`)),
	)
	if err != nil {
		panic(err)
	}

	for _, r := range rules {
		fmt.Println(r.ID, r.Action)
	}
	// Output:
	// r1 deny
	// r2 allow
}

// An oversized source is reported as ErrSourceTooLarge rather than as
// malformed content, so the two are distinguishable.
func ExampleErrSourceTooLarge() {
	loader, err := parserkit.NewLoader[Rule](&parserkit.LoadOptions[Rule]{MaxBytes: 8})
	if err != nil {
		panic(err)
	}

	_, err = loader.LoadOne(context.Background(), parserkit.BytesFetcher(`[{"id":"r1","action":"allow"}]`))
	fmt.Println(errors.Is(err, parserkit.ErrSourceTooLarge))
	// Output: true
}

// envFetcher reads a source out of the environment. Any type with a Fetch
// method is a source; ReadLimited is not needed for bytes already in memory,
// but the budget still has to be honoured.
type envFetcher string

func (e envFetcher) Fetch(_ context.Context, maxBytes int64) ([]byte, error) {
	raw := []byte(os.Getenv(string(e)))
	if int64(len(raw)) > maxBytes {
		return nil, fmt.Errorf("$%s: %w", string(e), parserkit.ErrSourceTooLarge)
	}
	return raw, nil
}

// Fetcher is the extension point: a source parser-kit has never heard of
// works the same as the ones it ships.
func ExampleFetcher() {
	_ = os.Setenv("APP_RULES", `[{"id":"env","action":"allow"}]`)
	defer func() { _ = os.Unsetenv("APP_RULES") }()

	loader, err := parserkit.NewLoader[Rule](nil)
	if err != nil {
		panic(err)
	}

	rules, err := loader.LoadOne(context.Background(), envFetcher("APP_RULES"))
	if err != nil {
		panic(err)
	}

	fmt.Println(rules[0].ID, rules[0].Action)
	// Output: env allow
}
