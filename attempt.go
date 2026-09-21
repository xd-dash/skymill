package skymill

import (
	"context"
	"errors"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
)

// Delivery identifies a durable consumer-group delivery independently of any
// process-local HTTP token.
type Delivery struct {
	Message       *message.Message
	StreamEntryID string
	ConsumerGroup string
	Consumer      string
	Deliveries    int64
	Idle          time.Duration
}

// DeliveryState asks Redis for the authoritative PEL record for an entry.
// Redis' delivery counter survives consumer death and XCLAIM, unlike message metadata.
func (s *Stream) DeliveryState(ctx context.Context, streamEntryID string) (Delivery, error) {
	if err := s.authorize(ctx, ActionInspect); err != nil { return Delivery{}, err }
	if s.group == "" { return Delivery{}, errors.New("skymill: delivery state requires a consumer group") }
	rows, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: s.binding.Stream, Group: s.group, Start: streamEntryID, End: streamEntryID, Count: 1,
	}).Result()
	if err != nil { return Delivery{}, err }
	if len(rows) == 0 || rows[0].ID != streamEntryID { return Delivery{}, errors.New("skymill: delivery is not pending") }
	return Delivery{StreamEntryID: rows[0].ID, ConsumerGroup: s.group, Consumer: rows[0].Consumer, Deliveries: rows[0].RetryCount, Idle: rows[0].Idle}, nil
}
