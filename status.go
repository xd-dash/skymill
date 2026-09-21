package skymill

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Status struct {
	Binding       Binding
	ConsumerGroup string
	StreamLength  int64
	Pending       PendingStats
	OldestPendingIdle time.Duration
	Consumers     int64
}

func (s *Stream) Status(ctx context.Context) (Status, error) {
	st := Status{Binding: s.binding, ConsumerGroup: s.group}
	length, err := s.client.XLen(ctx, s.binding.Stream).Result()
	if err != nil && err != redis.Nil { return Status{}, err }
	st.StreamLength = length
	if s.group == "" { return st, nil }
	pending, err := s.Pending(ctx)
	if err != nil { return Status{}, err }
	st.Pending = pending
	consumers, err := s.client.XInfoConsumers(ctx, s.binding.Stream, s.group).Result()
	if err != nil && err != redis.Nil { return Status{}, err }
	st.Consumers = int64(len(consumers))
	if pending.Count > 0 {
		rows, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: s.binding.Stream, Group: s.group, Start: "-", End: "+", Count: 1,
		}).Result()
		if err != nil && err != redis.Nil { return Status{}, err }
		if len(rows) > 0 { st.OldestPendingIdle = rows[0].Idle }
	}
	s.metric(ctx, "stream.length", float64(st.StreamLength), nil)
	s.metric(ctx, "delivery.pending", float64(st.Pending.Count), map[string]string{"consumer_group": s.group})
	s.metric(ctx, "delivery.oldest_pending_idle_ms", float64(st.OldestPendingIdle.Milliseconds()), map[string]string{"consumer_group": s.group})
	return st, nil
}
