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
	Hints      HintPublisher
	Logger     watermill.LoggerAdapter
}

type Stream struct {
	binding    Binding
	group      string
	authorizer Authorizer
	hints      HintPublisher
	client     redis.UniversalClient
	marshaller redisstream.Marshaller
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
	marshaller := redisstream.DefaultMarshallerUnmarshaller{}
	pub, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: config.Client, Marshaller: marshaller, Maxlens: maxlens,
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
		authorizer: config.Authorizer, hints: config.Hints, client: config.Client, marshaller: marshaller,
		publisher: pub, subscriber: sub,
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


type PublishResult struct {
	StreamEntryID string
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
	if s.authorizer != nil {
		if err := s.authorizer.AuthorizePublish(ctx, s.binding); err != nil {
			return PublishResult{}, err
		}
	}
	values, err := s.marshaller.Marshal(s.binding.Stream, msg)
	if err != nil {
		return PublishResult{}, err
	}
	uuid, _ := values[redisstream.UUIDHeaderKey].(string)
	metadata, _ := values["metadata"].([]byte)
	payload, _ := values["payload"].([]byte)
	idempotencyRedisKey := s.binding.idempotencyKey(idempotencyKey)
	const script = `
		local existing = redis.call('GET', KEYS[1])
		if existing then return {existing, '1'} end
		local id = redis.call('XADD', KEYS[2], '*',
			'_watermill_message_uuid', ARGV[1],
			'metadata', ARGV[2],
			'payload', ARGV[3])
		redis.call('SET', KEYS[1], id)
		return {id, '0'}
	`
	result, err := s.client.Eval(ctx, script, []string{idempotencyRedisKey, s.binding.Stream}, uuid, metadata, payload).Slice()
	if err != nil {
		return PublishResult{}, err
	}
	if len(result) != 2 {
		return PublishResult{}, errors.New("skymill: invalid publish-once result")
	}
	entryID, _ := result[0].(string)
	duplicate, _ := result[1].(string)
	pubResult := PublishResult{StreamEntryID: entryID, Duplicate: duplicate == "1"}
	s.emitHint(ctx, Hint{
		Kind: HintActivity, MessageID: msg.UUID, StreamEntryID: entryID, Duplicate: pubResult.Duplicate,
	})
	return pubResult, nil
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
