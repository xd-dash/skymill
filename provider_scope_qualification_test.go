package skymill

import (
 "strings"
 "testing"
)

// Qualify the package-owned Redis surface rather than copying a command
// vocabulary into the Fatline/Smoke qualification harness.
func TestScopedRedisRequirementsIsolation(t *testing.T) {
 a, err := ScopedRedisRequirements("world-17", "world-17:skymill:stream:github.webhooks")
 if err != nil { t.Fatal(err) }
 b, err := ScopedRedisRequirements("world-18", "world-18:skymill:stream:github.webhooks")
 if err != nil { t.Fatal(err) }
 if len(a.KeyPatterns) != 2 || len(b.KeyPatterns) != 2 { t.Fatalf("invalid scope families: %#v %#v", a, b) }
 for _, pattern := range a.KeyPatterns {
  if !strings.HasPrefix(pattern, "~world-17:skymill:") || strings.ContainsAny(strings.TrimPrefix(pattern, "~"), "?[]{}") {
   t.Fatalf("unsafe key authority: %q", pattern)
  }
  if pattern == b.KeyPatterns[0] || pattern == b.KeyPatterns[1] { t.Fatalf("cross-world overlap: %q", pattern) }
 }
 if a.KeyPatterns[0] == b.KeyPatterns[0] { t.Fatal("world stream collision") }
 required:=[]string{"get","set","eval","xadd","xreadgroup","xgroup","xpending","xclaim","xack"}
 actual:=make(map[string]bool,len(a.Commands))
 for _, cmd:=range a.Commands {
  lower:=strings.ToLower(cmd)
  if lower!=cmd || actual[cmd] { t.Fatalf("duplicate or noncanonical command %q",cmd) }
  if lower=="acl" || lower=="config" || lower=="function" || lower=="flushall" || lower=="flushdb" {t.Fatalf("admin command leaked: %q",cmd)}
  actual[cmd]=true
 }
 for _, cmd:=range required {if !actual[cmd] {t.Fatalf("required provider operation %q absent",cmd)}}
}
