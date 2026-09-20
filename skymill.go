package skymill

import (
	"context"
	"errors"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
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

// Authorizer is implemented by Fatline-facing policy adapters.
// Skymill owns stream mechanics; callers own authentication and policy.
type Authorizer interface {
	AuthorizePublish(context.Context, Binding) error
	AuthorizeConsume(context.Context, Binding, string) error
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
	Logger     watermill.LoggerAdapter
}

type Stream struct {
	binding    Binding
	group      string
	authorizer Authorizer
	publisher  *redisstream.Publisher
	subscriber *redisstream.Subscriber
}

func New(config Config) (*Stream, error) {
	if config.Client == nil {
		return nil, errors.New("skymill: redis client is required")
	}
	if config.Binding.Stream == "" {
		return nil, errors.New("skymill: stream name is required")
	}

	logger := config.Logger
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	maxlens := map[string]int64{}
	if config.MaxLen > 0 {
		maxlens[config.Binding.Stream] = config.MaxLen
	}
	pub, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client:  config.Client,
		Maxlens: maxlens,
	}, logger)
	if err != nil {
		return nil, err
	}

	sub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:                 config.Client,
		Consumer:               config.Consumer,
		ConsumerGroup:          config.ConsumerGroup,
		NackResendSleep:        config.NackResendSleep,
		BlockTime:              config.BlockTime,
		ClaimInterval:          config.ClaimInterval,
		ClaimBatchSize:         config.ClaimBatchSize,
		MaxIdleTime:            config.MaxIdleTime,
		ConsumerTimeout:        config.ConsumerTimeout,
		DisableIndefiniteInitialBlock: true,
	}, logger)
	if err != nil {
		_ = pub.Close()
		return nil, err
	}

	return &Stream{
		binding: config.Binding, group: config.ConsumerGroup,
		authorizer: config.Authorizer, publisher: pub, subscriber: sub,
	}, nil
}

func (s *Stream) Publish(ctx context.Context, messages ...*message.Message) error {
	if s.authorizer != nil {
		if err := s.authorizer.AuthorizePublish(ctx, s.binding); err != nil {
			return err
		}
	}
	for _, msg := range messages {
		if msg == nil {
			return errors.New("skymill: nil message")
		}
	}
	// Watermill Publisher has no context parameter; preserve it on each message.
	for _, msg := range messages {
		msg.SetContext(ctx)
	}
	return s.publisher.Publish(s.binding.Stream, messages...)
}

func (s *Stream) Subscribe(ctx context.Context) (<-chan *message.Message, error) {
	if s.authorizer != nil {
		if err := s.authorizer.AuthorizeConsume(ctx, s.binding, s.group); err != nil {
			return nil, err
		}
	}
	return s.subscriber.Subscribe(ctx, s.binding.Stream)
}

func (s *Stream) Close() error {
	var first error
	if err := s.subscriber.Close(); err != nil {
		first = err
	}
	if err := s.publisher.Close(); err != nil && first == nil {
		first = err
	}
	return first
}
