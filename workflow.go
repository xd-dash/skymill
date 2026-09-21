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
	ProviderDeliveryID string
	ConsumerGroup string
	State         WorkflowState
	ResultRef     string
	Error         string
	CreatedAt     time.Time
	SettledAt     *time.Time
}

func (s *Stream) Correlate(ctx context.Context, c Correlation) error {
	if c.ID == "" || c.MessageID == "" { return errors.New("skymill: correlation id and message id are required") }
	if err := s.authorize(ctx, ActionSettle); err != nil { return err }
	created, err := s.provider.CreateCorrelation(ctx, s.binding, c, s.policy.Workflow)
	if err != nil { return err }
	if created { s.metric(ctx, "workflow.correlated", 1, nil) }
	return nil
}

func (s *Stream) GetCorrelation(ctx context.Context, id string) (Correlation, error) {
	return s.provider.GetCorrelation(ctx, s.binding, id)
}

func (s *Stream) Settle(ctx context.Context, id string, state WorkflowState, resultRef, detail string) (Correlation, error) {
	if state != WorkflowSucceeded && state != WorkflowFailed { return Correlation{}, errors.New("skymill: settlement must be succeeded or failed") }
	if err := s.authorize(ctx, ActionSettle); err != nil { return Correlation{}, err }
	changed, err := s.provider.SettleCorrelation(ctx, s.binding, id, state, resultRef, detail)
	if err != nil { return Correlation{}, err }
	c, err := s.GetCorrelation(ctx, id)
	if err == nil && changed {
		s.metric(ctx, "workflow.settled", 1, map[string]string{"state": string(state)})
		s.emitHint(ctx, Hint{Kind: HintWorkflowSettled, MessageID: c.MessageID, ProviderDeliveryID: c.ProviderDeliveryID, ConsumerGroup: c.ConsumerGroup, Error: detail})
	}
	return c, err
}
