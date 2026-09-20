package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/xd-dash/skymill"
)

type Operation string

const (
	Publish Operation = "publish"
	Consume Operation = "consume"
	Settle Operation = "settle"
)

// Grant is the binding-derived authority established by authentication.
// Request bodies never select stream/group authority.
type Grant struct {
	Binding       skymill.Binding
	ConsumerGroup string
	Operations    map[Operation]bool
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

func requireGrant(r *http.Request, operation Operation, expected skymill.Binding, group string) error {
	g, ok := GrantFromContext(r.Context())
	if !ok { return errors.New("missing authority grant") }
	if !g.Operations[operation] { return errors.New("operation not authorized") }
	if g.Binding != expected { return errors.New("binding mismatch") }
	if operation != Publish && g.ConsumerGroup != group { return errors.New("consumer group mismatch") }
	return nil
}
