package skymill

import (
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
