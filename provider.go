package skymill

import (
	"encoding/base64"
	"errors"
	"regexp"
	"strings"
)

var scopeSegment = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
var streamName = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)

// RedisRequirements describes the authority of this scoped Watermill provider.
// It contains no Logma, Probot state, or Redis administration permissions.
type RedisRequirements struct {
	KeyPatterns []string
	Commands    []string
}

// ScopedRedisRequirements compiles a new scope-first stream binding.
// Legacy bindings remain readable using Config.Scope == ""; changing Scope
// selects a new deduplication namespace and is not a checkpoint migration.
func ScopedRedisRequirements(scope, stream string) (RedisRequirements, error) {
	prefix, err := scopedIdempotencyPrefix(scope, stream)
	if err != nil {
		return RedisRequirements{}, err
	}
	return RedisRequirements{
		KeyPatterns: []string{"~" + stream, "~" + prefix + "*"},
		Commands: []string{
			"ping", "hello", "client", "select",
			"get", "set", "eval", "xadd",
			"xread", "xreadgroup", "xgroup", "xpending", "xclaim", "xack", "xinfo",
		},
	}, nil
}

func scopedIdempotencyPrefix(scope, stream string) (string, error) {
	if !scopeSegment.MatchString(scope) || !streamName.MatchString(stream) ||
		!strings.HasPrefix(stream, scope+":skymill:stream:") ||
		len(stream) == len(scope+":skymill:stream:") {
		return "", errors.New("skymill: explicit scope and scoped stream name are required")
	}
	return scope + ":skymill:idempotency:" + base64.RawURLEncoding.EncodeToString([]byte(stream)) + ":", nil
}
