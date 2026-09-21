package skymill

import (
	"context"
	"errors"
)

type SettlementResult struct {
	Correlation Correlation
	Acked       bool
}

// SettleAndAck is the durable workflow completion primitive. It does not need
// the original Watermill Message or an HTTP delivery token: correlation stores
// the provider delivery identity and consumer group needed to settle the durable delivery.
//
// Settlement is idempotent and provider ACK is independently idempotent. The
// terminal correlation remains authoritative if a repeated ACK reports no pending entry.
func (s *Stream) SettleAndAck(ctx context.Context, id string, state WorkflowState, resultRef, detail string) (SettlementResult, error) {
	if state != WorkflowSucceeded && state != WorkflowFailed {
		return SettlementResult{}, errors.New("skymill: settlement must be succeeded or failed")
	}
	if err := s.authorize(ctx, ActionSettle); err != nil { return SettlementResult{}, err }
	c, err := s.GetCorrelation(ctx, id)
	if err != nil { return SettlementResult{}, err }
	if c.ID == "" { return SettlementResult{}, errors.New("skymill: unknown correlation") }
	if c.ProviderDeliveryID == "" || c.ConsumerGroup == "" { return SettlementResult{}, errors.New("skymill: correlation is not ack-capable") }
	if c.ConsumerGroup != s.group { return SettlementResult{}, errors.New("skymill: correlation consumer group mismatch") }

	if c.State == WorkflowPending {
		updated, err := s.Settle(ctx, id, state, resultRef, detail)
		if err != nil { return SettlementResult{}, err }
		c = updated
	}
	acked, err := s.provider.Ack(ctx, c.ConsumerGroup, c.ProviderDeliveryID)
	if err != nil { return SettlementResult{}, err }
	c, err = s.GetCorrelation(ctx, id)
	if err != nil { return SettlementResult{}, err }
	if acked {
		s.metric(ctx, "delivery.acked", 1, map[string]string{"consumer_group": c.ConsumerGroup, "source": "workflow"})
		s.emitHint(ctx, Hint{Kind: HintAcked, MessageID: c.MessageID, ProviderDeliveryID: c.ProviderDeliveryID, ConsumerGroup: c.ConsumerGroup})
	}
	return SettlementResult{Correlation: c, Acked: acked}, nil
}
