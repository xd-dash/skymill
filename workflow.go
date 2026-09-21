package skymill

import (
	"context"
	"errors"
	"time"
)

type WorkflowState string

const (
	WorkflowPending WorkflowState = "pending"
	WorkflowSucceeded WorkflowState = "succeeded"
	WorkflowFailed WorkflowState = "failed"
)

type Correlation struct {
	ID            string
	MessageID     string
	StreamEntryID string
	ConsumerGroup string
	State         WorkflowState
	ResultRef     string
	Error         string
	CreatedAt     time.Time
	SettledAt     *time.Time
}

func (s *Stream) Correlate(ctx context.Context, c Correlation) error {
	if c.ID == "" || c.MessageID == "" {
		return errors.New("skymill: correlation id and message id are required")
	}
	if err := s.authorize(ctx, ActionSettle); err != nil { return err }
	now := time.Now().UTC()
	key := s.binding.correlationKey(c.ID)
	fields := map[string]any{
		"id": c.ID, "message_id": c.MessageID, "stream_entry_id": c.StreamEntryID,
		"consumer_group": c.ConsumerGroup, "state": string(WorkflowPending),
		"created_at": now.Format(time.RFC3339Nano),
	}
	ok, err := s.client.HSetNX(ctx, key, "id", c.ID).Result()
	if err != nil { return err }
	if !ok { return nil }
	delete(fields, "id")
	if err := s.client.HSet(ctx, key, fields).Err(); err != nil { return err }
	if ttl := s.policy.Workflow.CorrelationTTL; ttl > 0 {
		if err := s.client.Expire(ctx, key, ttl).Err(); err != nil { return err }
	}
	s.metric(ctx, "workflow.correlated", 1, nil)
	return nil
}

func (s *Stream) GetCorrelation(ctx context.Context, id string) (Correlation, error) {
	row, err := s.client.HGetAll(ctx, s.binding.correlationKey(id)).Result()
	if err != nil { return Correlation{}, err }
	if len(row) == 0 { return Correlation{}, nil }
	c := Correlation{ID: row["id"], MessageID: row["message_id"], StreamEntryID: row["stream_entry_id"], ConsumerGroup: row["consumer_group"], State: WorkflowState(row["state"]), ResultRef: row["result_ref"], Error: row["error"]}
	c.CreatedAt, _ = time.Parse(time.RFC3339Nano, row["created_at"])
	if t, err := time.Parse(time.RFC3339Nano, row["settled_at"]); err == nil { c.SettledAt = &t }
	return c, nil
}

func (s *Stream) Settle(ctx context.Context, id string, state WorkflowState, resultRef, detail string) (Correlation, error) {
	if state != WorkflowSucceeded && state != WorkflowFailed {
		return Correlation{}, errors.New("skymill: settlement must be succeeded or failed")
	}
	if err := s.authorize(ctx, ActionSettle); err != nil { return Correlation{}, err }
	key := s.binding.correlationKey(id)
	now := time.Now().UTC()
	const script = `
		if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
		if redis.call('HGET', KEYS[1], 'state') ~= 'pending' then return 2 end
		redis.call('HSET', KEYS[1], 'state', ARGV[1], 'result_ref', ARGV[2], 'error', ARGV[3], 'settled_at', ARGV[4])
		return 1
	`
	result, err := s.client.Eval(ctx, script, []string{key}, string(state), resultRef, detail, now.Format(time.RFC3339Nano)).Int()
	if err != nil { return Correlation{}, err }
	if result == 0 { return Correlation{}, errors.New("skymill: unknown correlation") }
	c, err := s.GetCorrelation(ctx, id)
	if err == nil && result == 1 {
		s.metric(ctx, "workflow.settled", 1, map[string]string{"state": string(state)})
		s.emitHint(ctx, Hint{Kind: HintWorkflowSettled, MessageID: c.MessageID, StreamEntryID: c.StreamEntryID, ConsumerGroup: c.ConsumerGroup, Error: detail})
	}
	return c, err
}
