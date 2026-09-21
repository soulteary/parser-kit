package redissource_test

import (
	"context"
	"fmt"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	parserkit "github.com/soulteary/parser-kit/v3"
	"github.com/soulteary/parser-kit/v3/redissource"
)

type Rule struct {
	ID     string `json:"id"`
	Action string `json:"action"`
}

// Redis is the primary source, the file on disk the fallback.
func Example() {
	mr, err := miniredis.Run() // stand-in for a real server
	if err != nil {
		panic(err)
	}
	defer mr.Close()
	_ = mr.Set("app:rules", `[{"id":"r1","action":"allow"}]`)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	loader, err := parserkit.NewLoader[Rule](nil)
	if err != nil {
		panic(err)
	}

	rules, err := loader.Load(context.Background(),
		parserkit.At(0, redissource.New(rdb, "app:rules")),
		parserkit.At(1, parserkit.File("/etc/app/rules.json").AllowMissing()),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(rules[0].ID, rules[0].Action)
	// Output: r1 allow
}

// Getter is the part of a go-redis client the source uses -- two commands --
// so one source works on a standalone server, a Ring and a Sentinel or
// cluster setup behind redis.UniversalClient, with nothing to switch on.
func ExampleGetter() {
	mr, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer mr.Close()
	_ = mr.Set("app:rules", `[{"id":"r1","action":"allow"}]`)

	clients := []struct {
		shape  string
		client redissource.Getter
	}{
		{"Client", redis.NewClient(&redis.Options{Addr: mr.Addr()})},
		{"Ring", redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"one": mr.Addr()}})},
		{"UniversalClient", redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{mr.Addr()}})},
	}

	for _, c := range clients {
		raw, err := redissource.New(c.client, "app:rules").Fetch(context.Background(), parserkit.DefaultMaxBytes)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s: %s\n", c.shape, raw)
	}
	// Output:
	// Client: [{"id":"r1","action":"allow"}]
	// Ring: [{"id":"r1","action":"allow"}]
	// UniversalClient: [{"id":"r1","action":"allow"}]
}
