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
// the Redis Stream entry ID and consumer group needed to settle the PEL entry.
//
// The Lua operation orders state as terminal correlation -> XACK. Repeating it
// is safe. If XACK returns zero because the entry was already acknowledged, the
// terminal correlation remains the authoritative workflow record.
func (s *Stream) SettleAndAck(ctx context.Context, id string, state WorkflowState, resultRef, detail string) (SettlementResult, error) {
	if state != WorkflowSucceeded && state != WorkflowFailed {
		return SettlementResult{}, errors.New("skymill: settlement must be succeeded or failed")
	}
	if err := s.authorize(ctx, ActionSettle); err != nil { return SettlementResult{}, err }
	c, err := s.GetCorrelation(ctx, id)
	if err != nil { return SettlementResult{}, err }
	if c.ID == "" { return SettlementResult{}, errors.New("skymill: unknown correlation") }
	if c.StreamEntryID == "" || c.ConsumerGroup == "" { return SettlementResult{}, errors.New("skymill: correlation is not ack-capable") }
	if c.ConsumerGroup != s.group { return SettlementResult{}, errors.New("skymill: correlation consumer group mismatch") }

	transitioned := false
	if c.State == WorkflowPending {
		updated, err := s.Settle(ctx, id, state, resultRef, detail)
		if err != nil { return SettlementResult{}, err }
		c = updated
		transitioned = true
	}
	acked, err := s.provider.Ack(ctx, c.ConsumerGroup, c.StreamEntryID)
	if err != nil { return SettlementResult{}, err }
	c, err = s.GetCorrelation(ctx, id)
	if err != nil { return SettlementResult{}, err }
	if transitioned {
		s.metric(ctx, "workflow.settled", 1, map[string]string{"state": string(state)})
		s.emitHint(ctx, Hint{Kind: HintWorkflowSettled, MessageID: c.MessageID, StreamEntryID: c.StreamEntryID, ConsumerGroup: c.ConsumerGroup, Error: detail})
	}
	if acked {
		s.metric(ctx, "delivery.acked", 1, map[string]string{"consumer_group": c.ConsumerGroup, "source": "workflow"})
		s.emitHint(ctx, Hint{Kind: HintAcked, MessageID: c.MessageID, StreamEntryID: c.StreamEntryID, ConsumerGroup: c.ConsumerGroup})
	}
	return SettlementResult{Correlation: c, Acked: acked}, nil
}
