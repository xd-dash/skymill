package skymill

import (
	"context"
	"time"
)

type Status struct {
	Binding Binding
	ConsumerGroup string
	StreamLength int64
	Pending PendingStats
	OldestPendingIdle time.Duration
	Consumers int64
}

func (s *Stream) Status(ctx context.Context) (Status, error) {
	if err := s.authorize(ctx, ActionInspect); err != nil { return Status{}, err }
	p, err := s.provider.Status(ctx)
	if err != nil { return Status{}, err }
	st := Status{Binding:s.binding, ConsumerGroup:s.group, StreamLength:p.StreamLength, Pending:p.Pending, OldestPendingIdle:p.OldestPendingIdle, Consumers:p.Consumers}
	s.metric(ctx, "stream.length", float64(st.StreamLength), nil)
	s.metric(ctx, "delivery.pending", float64(st.Pending.Count), map[string]string{"consumer_group":s.group})
	s.metric(ctx, "delivery.oldest_pending_idle_ms", float64(st.OldestPendingIdle.Milliseconds()), map[string]string{"consumer_group":s.group})
	return st,nil
}
