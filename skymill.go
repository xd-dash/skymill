package skymill

import (
	"context"
	"errors"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
)

// Binding describes the authority-relevant identity of a stream.
// It deliberately does not contain Redis-specific queue mechanics.
type Binding struct {
	Org         string
	Tenant      string
	Application string
	Stream      string
}

type Config struct {
	Client redis.UniversalClient

	Binding Binding

	// ConsumerGroup enables durable competing-consumer semantics.
	// An empty group uses the provider's fan-out mode.
	ConsumerGroup string
	Consumer      string

	MaxLen int64

	NackResendSleep time.Duration
	BlockTime       time.Duration
	ClaimInterval   time.Duration
	ClaimBatchSize  int64
	MaxIdleTime     time.Duration
	ConsumerTimeout time.Duration

	Authorizer Authorizer
	Hints      HintPublisher
	Metrics    Metrics
	Policy     Policy
	Logger     watermill.LoggerAdapter
}

type Stream struct {
	binding    Binding
	group      string
	authorizer Authorizer
	hints      HintPublisher
	metrics    Metrics
	policy     Policy
	provider DurableProvider
}

func New(config Config) (*Stream, error) {
	if config.Client == nil {
		return nil, errors.New("skymill: redis client is required")
	}
	if err := config.Policy.validate(config.Binding); err != nil {
		return nil, err
	}
	if config.Binding.Stream == "" {
		return nil, errors.New("skymill: stream name is required")
	}

	maxLen := config.MaxLen
	if config.Policy.Retention.MaxLen > 0 { maxLen = config.Policy.Retention.MaxLen }
	provider, err := newRedisStreamProvider(RedisStreamProviderConfig{
		Client:config.Client, Binding:config.Binding, ConsumerGroup:config.ConsumerGroup, Consumer:config.Consumer,
		MaxLen:maxLen, NackResendSleep:config.NackResendSleep, BlockTime:config.BlockTime,
		ClaimInterval:config.ClaimInterval, ClaimBatchSize:config.ClaimBatchSize, MaxIdleTime:config.MaxIdleTime,
		ConsumerTimeout:config.ConsumerTimeout, Logger:config.Logger,
	})
	if err != nil { return nil, err }

	return &Stream{
		binding: config.Binding, group: config.ConsumerGroup,
		authorizer: config.Authorizer, hints: config.Hints, metrics: config.Metrics, policy: config.Policy, provider: provider,
	}, nil
}

func (s *Stream) Publish(ctx context.Context, messages ...*message.Message) error {
	if err := s.authorize(ctx, ActionPublish); err != nil { return err }
	for _, msg := range messages {
		if msg == nil {
			return errors.New("skymill: nil message")
		}
	}
	// Watermill Publisher has no context parameter; preserve it on each message.
	for _, msg := range messages {
		msg.SetContext(ctx)
	}
	return s.provider.Publish(ctx, messages...)
}


type PublishResult struct {
	ProviderDeliveryID string
	Duplicate     bool
}

// PublishOnce atomically couples a caller-supplied idempotency key to XADD.
// It is the durable ingress primitive for sources such as GitHub whose
// delivery identity must survive ambiguous HTTP retries.
func (s *Stream) PublishOnce(ctx context.Context, idempotencyKey string, msg *message.Message) (PublishResult, error) {
	if idempotencyKey == "" {
		return PublishResult{}, errors.New("skymill: idempotency key is required")
	}
	if msg == nil {
		return PublishResult{}, errors.New("skymill: nil message")
	}
	if err := s.authorize(ctx, ActionPublish); err != nil { return PublishResult{}, err }
	pubResult, err := s.provider.PublishOnce(ctx, idempotencyKey, msg, s.policy.Retention)
	if err != nil { return PublishResult{}, err }
	s.metric(ctx, "publish.accepted", 1, map[string]string{"duplicate": map[bool]string{true:"1",false:"0"}[pubResult.Duplicate]})
	s.emitHint(ctx, Hint{
		Kind: HintActivity, MessageID: msg.UUID, ProviderDeliveryID: pubResult.ProviderDeliveryID, Duplicate: pubResult.Duplicate,
	})
	return pubResult, nil
}

func (s *Stream) Binding() Binding { return s.binding }
func (s *Stream) ConsumerGroup() string { return s.group }

func (s *Stream) Subscribe(ctx context.Context) (<-chan Delivery, error) {
	if err := s.authorize(ctx, ActionConsume); err != nil { return nil, err }
	return s.provider.Subscribe(ctx)
}

func (s *Stream) Close() error { return s.provider.Close() }
