package skymill

import (
	"crypto/sha256"
	"encoding/hex"
)

func (b Binding) redisSlotTag() string {
	sum := sha256.Sum256([]byte(b.Org + "\x00" + b.Tenant + "\x00" + b.Application + "\x00" + b.Stream))
	return hex.EncodeToString(sum[:16])
}

func (b Binding) redisStreamKey() string {
	return "skymill:{" + b.redisSlotTag() + "}:stream:" + b.Stream
}

func (b Binding) redisAuxStreamKey(name string) string {
	return "skymill:{" + b.redisSlotTag() + "}:stream:" + name
}

func (b Binding) idempotencyKey(id string) string {
	sum := sha256.Sum256([]byte(id))
	return "skymill:{" + b.redisSlotTag() + "}:idempotency:" + hex.EncodeToString(sum[:])
}

func (b Binding) correlationKey(id string) string {
	sum := sha256.Sum256([]byte(id))
	return "skymill:{" + b.redisSlotTag() + "}:correlation:" + hex.EncodeToString(sum[:])
}
