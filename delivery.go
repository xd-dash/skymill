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
	s.emitHint(ctx, Hint{Kind: HintDeadLettered, MessageID: msg.UUID, StreamEntryID: providerDeliveryID, ConsumerGroup: s.group})
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
