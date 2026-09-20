package skymill

import (
	"crypto/sha256"
	"encoding/hex"
)

func (b Binding) idempotencyKey(id string) string {
	// Hash caller-controlled identity so Redis key syntax, size, and accidental
	// cross-scope collisions are independent of source identifiers.
	sum := sha256.Sum256([]byte(b.Org + "\x00" + b.Tenant + "\x00" + b.Application + "\x00" + b.Stream + "\x00" + id))
	return "skymill:idempotency:" + hex.EncodeToString(sum[:])
}
