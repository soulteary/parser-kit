package remotesource_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	parserkit "github.com/soulteary/parser-kit/v2"
	"github.com/soulteary/parser-kit/v2/remotesource"
)

type Rule struct {
	ID     string `json:"id"`
	Action string `json:"action"`
}

// A remote source with a local file behind it: if the endpoint is down, the
// file on disk answers.
func Example() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"r1","action":"allow"}]`))
	}))
	defer srv.Close()

	remote, err := remotesource.New(srv.URL+"/rules.json",
		remotesource.WithAuthorization("Bearer token"),
		remotesource.WithTimeout(3*time.Second),
	)
	if err != nil {
		panic(err)
	}

	loader, err := parserkit.NewLoader[Rule](nil)
	if err != nil {
		panic(err)
	}

	rules, err := loader.Load(context.Background(),
		parserkit.At(0, remote),
		parserkit.At(1, parserkit.File("/etc/app/rules.json").AllowMissing()),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(rules[0].ID, rules[0].Action)
	// Output: r1 allow
}

// Retries back off. A non-positive field is replaced by its default rather
// than honoured, because zero means "retry immediately, forever" -- see
// RetryPolicy.
func ExampleRetryPolicy() {
	f, err := remotesource.New("https://config.internal/rules.json",
		remotesource.WithRetry(remotesource.RetryPolicy{MaxRetries: 5}),
	)
	if err != nil {
		panic(err)
	}

	p := f.Retry()
	fmt.Println(p.MaxRetries, p.RetryDelay, p.MaxRetryDelay)
	// Output: 5 1s 30s
}
