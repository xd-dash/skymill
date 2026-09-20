package skymill

import (
	"context"
	"time"
)

// Hint is an ephemeral observation about authoritative stream state.
// Consumers must never use receipt of a Hint as proof that durable work exists
// or has completed; they must verify the stream/application state.
type Hint struct {
	Kind          string
	Binding       Binding
	MessageID     string
	StreamEntryID string
	ConsumerGroup string
	Duplicate     bool
	Error         string
	At            time.Time
}

// HintPublisher is intentionally smaller than Logma's full API. A Logma
// adapter can implement it without making Skymill depend on Logma itself.
type HintPublisher interface {
	PublishHint(context.Context, Hint) error
}

const (
	HintActivity = "stream.activity"
	HintAcked    = "stream.acked"
	HintNacked   = "stream.nacked"
	HintWorkflowSettled = "stream.workflow.settled"
	HintDeadLettered = "stream.dead_lettered"
)

func (s *Stream) emitHint(ctx context.Context, hint Hint) {
	if s.hints == nil {
		return
	}
	hint.Binding = s.binding
	if hint.At.IsZero() {
		hint.At = time.Now().UTC()
	}
	// Hints are deliberately best-effort. Durable stream operations have
	// already succeeded (or, for completion hints, the delivery state has
	// already transitioned) before this is called.
	_ = s.hints.PublishHint(ctx, hint)
}

func (s *Stream) EmitCompletionHint(ctx context.Context, kind, messageID, detail string) {
	if kind != HintAcked && kind != HintNacked {
		return
	}
	s.emitHint(ctx, Hint{
		Kind: kind, MessageID: messageID, ConsumerGroup: s.group, Error: detail,
	})
}
