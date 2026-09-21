package skymill

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

// DurableProvider is the internal durability boundary. Stream owns policy,
// authority, workflow semantics, hints, and metrics; providers own durable
// transport identity and mechanics.
type DurableProvider interface {
	Publish(context.Context, ...*message.Message) error
	PublishOnce(context.Context, string, *message.Message, RetentionPolicy) (PublishResult, error)
	Subscribe(context.Context) (<-chan *message.Message, error)

	DeliveryState(context.Context, string) (Delivery, error)
	DeadLetter(context.Context, Delivery, *message.Message, string) error
	Ack(context.Context, string, string) (bool, error)
	Status(context.Context) (ProviderStatus, error)

	Close() error
}

type ProviderStatus struct {
	StreamLength      int64
	Pending           PendingStats
	OldestPendingIdle time.Duration
	Consumers         int64
}
