package skymill

import (
	"context"
	"errors"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
)

// Delivery is Skymill's provider-neutral durable delivery envelope.
// ProviderDeliveryID is opaque outside the provider adapter. For Redis Streams
// it is the stream entry ID; other providers may encode their own durable identity.
type Delivery struct {
	Message            *message.Message
	ProviderDeliveryID string
	ConsumerGroup      string
	Consumer           string
	Attempt            int64
	Idle               time.Duration
}

func (s *Stream) DeliveryState(ctx context.Context, providerDeliveryID string) (Delivery, error) {
	if err := s.authorize(ctx, ActionInspect); err != nil { return Delivery{}, err }
	if s.group == "" { return Delivery{}, errors.New("skymill: delivery state requires a consumer group") }
	rows, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: s.binding.Stream, Group: s.group, Start: providerDeliveryID, End: providerDeliveryID, Count: 1,
	}).Result()
	if err != nil { return Delivery{}, err }
	if len(rows) == 0 || rows[0].ID != providerDeliveryID { return Delivery{}, errors.New("skymill: delivery is not pending") }
	return Delivery{
		ProviderDeliveryID: rows[0].ID, ConsumerGroup: s.group,
		Consumer: rows[0].Consumer, Attempt: rows[0].RetryCount, Idle: rows[0].Idle,
	}, nil
}
