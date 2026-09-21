package skymill

import "context"

type Action string

const (
	ActionPublish Action = "publish"
	ActionConsume Action = "consume"
	ActionSettle  Action = "settle"
	ActionInspect Action = "inspect"
)

type AuthorizationRequest struct {
	Binding       Binding
	ConsumerGroup string
	Action        Action
}

type Authorizer interface {
	Authorize(context.Context, AuthorizationRequest) error
}

func (s *Stream) authorize(ctx context.Context, action Action) error {
	if s.authorizer == nil { return nil }
	return s.authorizer.Authorize(ctx, AuthorizationRequest{
		Binding: s.binding, ConsumerGroup: s.group, Action: action,
	})
}
