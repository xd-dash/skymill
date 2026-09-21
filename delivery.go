package skymill

import (
	"context"
	"errors"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/redis/go-redis/v9"
)

func (s *Stream) PrepareDelivery(ctx context.Context, streamEntryID string, msg *message.Message) (bool, error) {
	if err := s.authorize(ctx, ActionInspect); err != nil { return false, err }
	if msg == nil { return false, errors.New("skymill: nil delivery") }
	if s.policy.Retry.MaxDeliveries == 0 { return true, nil }
	state, err := s.DeliveryState(ctx, streamEntryID)
	if err != nil { return false, err }
	s.metric(ctx, "delivery.attempt", float64(state.Deliveries), map[string]string{"consumer_group": s.group})
	if state.Deliveries <= s.policy.Retry.MaxDeliveries { return true, nil }

	values, err := s.marshaller.Marshal(s.policy.Retry.DeadLetterStream, msg)
	if err != nil { return false, err }
	uuid, _ := values[redisstream.UUIDHeaderKey].(string)
	metadata, _ := values["metadata"].([]byte)
	payload, _ := values["payload"].([]byte)
	const script = `
		local existing = redis.call('GET', KEYS[1])
		if existing then return existing end
		local id = redis.call('XADD', KEYS[2], '*',
			'_watermill_message_uuid', ARGV[1], 'metadata', ARGV[2], 'payload', ARGV[3])
		redis.call('SET', KEYS[1], id)
		return id
	`
	key := s.binding.idempotencyKey("dlq:" + s.group + ":" + streamEntryID)
	if _, err := s.client.Eval(ctx, script, []string{key, s.policy.Retry.DeadLetterStream}, uuid, metadata, payload).Result(); err != nil { return false, err }
	s.metric(ctx, "delivery.dead_lettered", 1, map[string]string{"consumer_group": s.group})
	s.emitHint(ctx, Hint{Kind: HintDeadLettered, MessageID: msg.UUID, StreamEntryID: streamEntryID, ConsumerGroup: s.group})
	return false, nil
}

type PendingStats struct {
	Count int64
	LowestID string
	HighestID string
	Consumers int64
}

func (s *Stream) Pending(ctx context.Context) (PendingStats, error) {
	if err := s.authorize(ctx, ActionInspect); err != nil { return PendingStats{}, err }
	if s.group == "" { return PendingStats{}, errors.New("skymill: pending stats require a consumer group") }
	p, err := s.client.XPending(ctx, s.binding.Stream, s.group).Result()
	if err != nil && err != redis.Nil { return PendingStats{}, err }
	if p == nil { return PendingStats{}, nil }
	return PendingStats{Count: p.Count, LowestID: p.Lower, HighestID: p.Higher, Consumers: int64(len(p.Consumers))}, nil
}
