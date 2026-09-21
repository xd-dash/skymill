package skymill

import (
	"context"
	"errors"
	"time"
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
	if s.authorizer != nil {
		if err := s.authorizer.AuthorizeSettle(ctx, s.binding, s.group); err != nil { return SettlementResult{}, err }
	}
	c, err := s.GetCorrelation(ctx, id)
	if err != nil { return SettlementResult{}, err }
	if c.ID == "" { return SettlementResult{}, errors.New("skymill: unknown correlation") }
	if c.StreamEntryID == "" || c.ConsumerGroup == "" { return SettlementResult{}, errors.New("skymill: correlation is not ack-capable") }
	if c.ConsumerGroup != s.group { return SettlementResult{}, errors.New("skymill: correlation consumer group mismatch") }

	now := time.Now().UTC()
	key := s.binding.correlationKey(id)
	const script = `
		if redis.call('EXISTS', KEYS[1]) == 0 then return {-1, 0} end
		local current = redis.call('HGET', KEYS[1], 'state')
		if current == 'pending' then
			redis.call('HSET', KEYS[1], 'state', ARGV[1], 'result_ref', ARGV[2], 'error', ARGV[3], 'settled_at', ARGV[4])
		end
		local acked = redis.call('XACK', KEYS[2], ARGV[5], ARGV[6])
		return {current == 'pending' and 1 or 0, acked}
	`
	raw, err := s.client.Eval(ctx, script, []string{key, s.binding.Stream}, string(state), resultRef, detail, now.Format(time.RFC3339Nano), c.ConsumerGroup, c.StreamEntryID).Slice()
	if err != nil { return SettlementResult{}, err }
	if len(raw) != 2 { return SettlementResult{}, errors.New("skymill: invalid settle-and-ack result") }
	transitioned, _ := raw[0].(int64)
	acked, _ := raw[1].(int64)
	c, err = s.GetCorrelation(ctx, id)
	if err != nil { return SettlementResult{}, err }
	if transitioned == 1 {
		s.metric(ctx, "workflow.settled", 1, map[string]string{"state": string(state)})
		s.emitHint(ctx, Hint{Kind: HintWorkflowSettled, MessageID: c.MessageID, StreamEntryID: c.StreamEntryID, ConsumerGroup: c.ConsumerGroup, Error: detail})
	}
	if acked > 0 {
		s.metric(ctx, "delivery.acked", 1, map[string]string{"consumer_group": c.ConsumerGroup, "source": "workflow"})
		s.emitHint(ctx, Hint{Kind: HintAcked, MessageID: c.MessageID, StreamEntryID: c.StreamEntryID, ConsumerGroup: c.ConsumerGroup})
	}
	return SettlementResult{Correlation: c, Acked: acked > 0}, nil
}
