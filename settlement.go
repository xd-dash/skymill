package skymill

import (
	"context"
	"errors"
)

type SettlementResult struct {
	Correlation Correlation
	Acked       bool
}

func (s *Stream) SettleAndAck(ctx context.Context, id string, state WorkflowState, resultRef, detail string) (SettlementResult, error) {
	if state != WorkflowSucceeded && state != WorkflowFailed {
		return SettlementResult{}, errors.New("skymill: settlement must be succeeded or failed")
	}
	if err := s.authorize(ctx, ActionSettle); err != nil { return SettlementResult{}, err }

	c, changed, acked, err := s.provider.SettleAndAck(ctx, s.binding, id, state, resultRef, detail)
	if err != nil { return SettlementResult{}, err }
	if c.ConsumerGroup != s.group {
		return SettlementResult{}, errors.New("skymill: correlation consumer group mismatch")
	}
	if changed {
		s.metric(ctx, "workflow.settled", 1, map[string]string{"state": string(c.State)})
		s.emitHint(ctx, Hint{Kind: HintWorkflowSettled, MessageID: c.MessageID, ProviderDeliveryID: c.ProviderDeliveryID, ConsumerGroup: c.ConsumerGroup, Error: detail})
	}
	if acked {
		s.metric(ctx, "delivery.acked", 1, map[string]string{"consumer_group": c.ConsumerGroup, "source": "workflow"})
		s.emitHint(ctx, Hint{Kind: HintAcked, MessageID: c.MessageID, ProviderDeliveryID: c.ProviderDeliveryID, ConsumerGroup: c.ConsumerGroup})
	}
	return SettlementResult{Correlation:c,Acked:acked},nil
}
