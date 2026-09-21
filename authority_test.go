package skymill

import (
	"context"
	"errors"
	"testing"
)

type recordingAuthorizer struct {
	requests []AuthorizationRequest
	deny Action
}

func (a *recordingAuthorizer) Authorize(_ context.Context, req AuthorizationRequest) error {
	a.requests = append(a.requests, req)
	if req.Action == a.deny { return errors.New("denied") }
	return nil
}

func TestAuthorizeCarriesBindingGroupAndAction(t *testing.T) {
	a := &recordingAuthorizer{}
	s := &Stream{binding: Binding{Org:"o", Tenant:"t", Application:"a", Stream:"s"}, group:"g", authorizer:a}
	if err := s.authorize(context.Background(), ActionInspect); err != nil { t.Fatal(err) }
	if len(a.requests) != 1 { t.Fatalf("requests=%d", len(a.requests)) }
	got := a.requests[0]
	if got.Action != ActionInspect || got.ConsumerGroup != "g" || got.Binding != s.binding { t.Fatalf("unexpected request: %#v", got) }
}

func TestAuthorizeDenialPropagates(t *testing.T) {
	a := &recordingAuthorizer{deny:ActionSettle}
	s := &Stream{binding: Binding{Stream:"s"}, group:"g", authorizer:a}
	if err := s.authorize(context.Background(), ActionSettle); err == nil { t.Fatal("expected denial") }
}
