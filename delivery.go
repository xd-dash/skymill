package skymill

import (
	"context"
	"errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

func (s *Stream) PrepareDelivery(ctx context.Context, providerDeliveryID string, msg *message.Message) (bool, error) {
	if err := s.authorize(ctx, ActionInspect); err != nil { return false, err }
	if msg == nil { return false, errors.New("skymill: nil delivery") }
	if s.policy.Retry.MaxDeliveries == 0 { return true, nil }
	state, err := s.DeliveryState(ctx, providerDeliveryID)
	if err != nil { return false, err }
	s.metric(ctx, "delivery.attempt", float64(state.Attempt), map[string]string{"consumer_group": s.group})
	if state.Attempt <= s.policy.Retry.MaxDeliveries { return true, nil }

	if err := s.provider.DeadLetter(ctx, state, msg, s.policy.Retry.DeadLetterStream); err != nil { return false, err }
	s.metric(ctx, "delivery.dead_lettered", 1, map[string]string{"consumer_group": s.group})
	s.emitHint(ctx, Hint{Kind: HintDeadLettered, MessageID: msg.UUID, ProviderDeliveryID: providerDeliveryID, ConsumerGroup: s.group})
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
	status, err := s.provider.Status(ctx)
	if err != nil { return PendingStats{}, err }
	return status.Pending, nil
}


func (s *Stream) AckDelivery(ctx context.Context, d Delivery) (bool, error) {
	if err := s.authorize(ctx, ActionConsume); err != nil { return false, err }
	if d.ConsumerGroup != s.group { return false, errors.New("skymill: delivery consumer group mismatch") }
	acked, err := s.provider.Ack(ctx, d.ConsumerGroup, d.ProviderDeliveryID)
	if err != nil { return false, err }
	if acked {
		s.metric(ctx, "delivery.acked", 1, map[string]string{"consumer_group":s.group})
		messageID:=""; if d.Message!=nil { messageID=d.Message.UUID }
		s.emitHint(ctx, Hint{Kind:HintAcked,MessageID:messageID,ProviderDeliveryID:d.ProviderDeliveryID,ConsumerGroup:d.ConsumerGroup})
	}
	return acked,nil
}

func (s *Stream) NackDelivery(ctx context.Context, d Delivery) error {
	if err := s.authorize(ctx, ActionConsume); err != nil { return err }
	if d.ConsumerGroup != s.group { return errors.New("skymill: delivery consumer group mismatch") }
	if err := s.provider.Nack(ctx,d); err != nil { return err }
	s.metric(ctx, "delivery.nacked", 1, map[string]string{"consumer_group":s.group})
	messageID:=""
	if d.Message!=nil { messageID=d.Message.UUID }
	s.emitHint(ctx, Hint{Kind:HintNacked,MessageID:messageID,ProviderDeliveryID:d.ProviderDeliveryID,ConsumerGroup:d.ConsumerGroup})
	return nil
}
