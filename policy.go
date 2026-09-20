package skymill

import (
	"errors"
	"time"
)

type RetentionPolicy struct {
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
