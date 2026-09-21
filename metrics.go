package skymill

import "context"

type Metric struct {
	Name    string
	Binding Binding
	Value   float64
	Labels  map[string]string
}

// Metrics is deliberately exporter-neutral. Fatline can adapt this to its
// metrics backend without making Skymill own an observability stack.
type Metrics interface {
	Observe(context.Context, Metric)
}

func (s *Stream) metric(ctx context.Context, name string, value float64, labels map[string]string) {
	if s.metrics == nil { return }
	s.metrics.Observe(ctx, Metric{Name: name, Binding: s.binding, Value: value, Labels: labels})
}

func (s *Stream) ObserveAck(ctx context.Context, ack bool) {
	name := "delivery.nacked"
	if ack { name = "delivery.acked" }
	s.metric(ctx, name, 1, map[string]string{"consumer_group": s.group})
}
