package skymill

import (
	"errors"
	"time"
)

type RetentionMode string

const (
	// RetentionOperational accepts that count-based stream trimming and
	// time-based idempotency retention are independent operational bounds.
	RetentionOperational RetentionMode = "operational"
	// RetentionReplaySafe disables count trimming. Idempotency records therefore
	// cannot point at entries removed by Skymill itself; external Redis retention
	// must obey the same contract.
	RetentionReplaySafe RetentionMode = "replay-safe"
)

type RetentionPolicy struct {
	Mode RetentionMode
	// IdempotencyTTL bounds source-deduplication state. Zero means retain.
	IdempotencyTTL time.Duration
	// MaxLen requests approximate Redis Stream trimming through Watermill.
	MaxLen int64
}

type RetryPolicy struct {
	// MaxDeliveries is the maximum observed delivery count before dead-letter.
	// Zero means provider redelivery remains unbounded and is discouraged for services.
	MaxDeliveries int64
	DeadLetterStream string
}

type WorkflowPolicy struct {
	// CorrelationTTL bounds settled workflow correlation records. Zero means retain.
	CorrelationTTL time.Duration
}

type Policy struct {
	Retention RetentionPolicy
	Retry     RetryPolicy
	Workflow  WorkflowPolicy
}

func (p Policy) validate(binding Binding) error {
	if p.Retention.IdempotencyTTL < 0 || p.Workflow.CorrelationTTL < 0 {
		return errors.New("skymill: retention durations must be non-negative")
	}
	if p.Retention.Mode == "" { p.Retention.Mode = RetentionOperational }
	if p.Retention.Mode != RetentionOperational && p.Retention.Mode != RetentionReplaySafe {
		return errors.New("skymill: unknown retention mode")
	}
	if p.Retention.Mode == RetentionReplaySafe && p.Retention.MaxLen > 0 {
		return errors.New("skymill: replay-safe retention cannot use count-based MaxLen")
	}
	if p.Retention.MaxLen < 0 || p.Retry.MaxDeliveries < 0 {
		return errors.New("skymill: retention/retry counts must be non-negative")
	}
	if p.Retry.MaxDeliveries > 0 && p.Retry.DeadLetterStream == "" {
		return errors.New("skymill: bounded retries require a dead-letter stream")
	}
	if p.Retry.DeadLetterStream == binding.Stream {
		return errors.New("skymill: dead-letter stream must differ from source stream")
	}
	return nil
}
