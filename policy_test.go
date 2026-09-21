package skymill

import "testing"

func TestReplaySafeRejectsCountTrim(t *testing.T) {
	p := Policy{Retention:RetentionPolicy{Mode:RetentionReplaySafe, MaxLen:100}}
	if err := p.validate(Binding{Stream:"s"}); err == nil { t.Fatal("expected replay-safe MaxLen rejection") }
}

func TestOperationalAllowsIndependentBounds(t *testing.T) {
	p := Policy{Retention:RetentionPolicy{Mode:RetentionOperational, MaxLen:100}}
	if err := p.validate(Binding{Stream:"s"}); err != nil { t.Fatal(err) }
}

func TestRetryRequiresDistinctDLQ(t *testing.T) {
	p := Policy{Retry:RetryPolicy{MaxDeliveries:3, DeadLetterStream:"s"}}
	if err := p.validate(Binding{Stream:"s"}); err == nil { t.Fatal("expected same-stream DLQ rejection") }
}
