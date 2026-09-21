package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/xd-dash/skymill"
)

// Grant is the compiled authority established by authentication. It mirrors
// Skymill actions rather than defining a second HTTP-specific ACL vocabulary.
type Grant struct {
	Binding       skymill.Binding
	ConsumerGroup string
	Actions       map[skymill.Action]bool
	Principal     string
}

type grantKey struct{}

func WithGrant(ctx context.Context, grant Grant) context.Context {
	return context.WithValue(ctx, grantKey{}, grant)
}

func GrantFromContext(ctx context.Context) (Grant, bool) {
	g, ok := ctx.Value(grantKey{}).(Grant)
	return g, ok
}

func requireGrant(r *http.Request, action skymill.Action, expected skymill.Binding, group string) error {
	g, ok := GrantFromContext(r.Context())
	if !ok { return errors.New("missing authority grant") }
	if !g.Actions[action] { return errors.New("action not authorized") }
	if g.Binding != expected { return errors.New("binding mismatch") }
	if action != skymill.ActionPublish && g.ConsumerGroup != group { return errors.New("consumer group mismatch") }
	return nil
}
