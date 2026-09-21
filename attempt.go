package skymill

import (
	"context"
	"errors"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
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
	return s.provider.DeliveryState(ctx, providerDeliveryID)
}
