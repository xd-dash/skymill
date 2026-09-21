package skymill

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// DurableProvider is the internal durability boundary. Stream owns authority,
// policy, workflow semantics, hints, and metrics; the provider owns transport
// identity, durable delivery mechanics, and atomic settlement with that delivery.
type DurableProvider interface {
	Publish(context.Context, ...*message.Message) error
	PublishOnce(context.Context, string, *message.Message, RetentionPolicy) (PublishResult, error)
	Subscribe(context.Context) (<-chan *message.Message, error)

	DeliveryState(context.Context, string) (Delivery, error)
	DeadLetter(context.Context, Delivery, *message.Message, string) error
	Ack(context.Context, string, string) (bool, error)
	Status(context.Context) (ProviderStatus, error)

	CreateCorrelation(context.Context, Binding, Correlation, WorkflowPolicy) (bool, error)
	GetCorrelation(context.Context, Binding, string) (Correlation, error)
	SettleCorrelation(context.Context, Binding, string, WorkflowState, string, string) (bool, error)
	SettleAndAck(context.Context, Binding, string, WorkflowState, string, string) (Correlation, bool, bool, error)

	Close() error
}

type ProviderStatus struct {
	StreamLength      int64
	Pending           PendingStats
	OldestPendingIdle time.Duration
	Consumers         int64
}
