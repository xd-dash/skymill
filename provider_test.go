package skymill

import (
	"strings"
	"testing"
)

func TestScopedRedisRequirements(t *testing.T) {
	req, err := ScopedRedisRequirements("world-17", "world-17:skymill:stream:github.webhooks")
	if err != nil {
		t.Fatal(err)
	}
	if len(req.KeyPatterns) != 2 || req.KeyPatterns[0] != "~world-17:skymill:stream:github.webhooks" ||
		!strings.HasPrefix(req.KeyPatterns[1], "~world-17:skymill:idempotency:") {
		t.Fatalf("requirements=%+v", req)
	}
	for _, invalid := range []struct{ scope, stream string }{
		{"", "github.webhooks"},
		{"world*", "world*:skymill:stream:probe"},
		{"world-17", "world-18:skymill:stream:probe"},
		{"world-17", "world-17:skymill:stream:"},
		{"world-17", "world-17:skymill:stream:*"},
		{"world-17", "world-17:probot:probe"},
	} {
		if _, err := ScopedRedisRequirements(invalid.scope, invalid.stream); err == nil {
			t.Fatalf("invalid binding compiled: %+v", invalid)
		}
	}
	a, _ := scopedIdempotencyPrefix("world-17", "world-17:skymill:stream:a")
	b, _ := scopedIdempotencyPrefix("world-17", "world-17:skymill:stream:a:b")
	if a == b || strings.HasPrefix(b, a) {
		t.Fatal("distinct stream identities share deduplication authority")
	}
}
