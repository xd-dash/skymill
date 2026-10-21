package httpapi

import (
 "context"
 "errors"
 "fmt"

 "github.com/xd-dash/skymill"
)

// ProjectAuthority is a verified, policy-bound project identity, not an
// untrusted request claim. Prajapati's VerifiedProject satisfies this contract.
type ProjectAuthority interface {
 ProjectID() string
 Authorize(action, resource string) error
}

// ProjectGrant compiles a verified project policy into Skymill's existing
// stream grant. A project may only access a stream with an identical tenant.
// The policy must explicitly authorize every action and exact stream resource.
func ProjectGrant(ctx context.Context, identity ProjectAuthority, stream skymill.Binding, group string) (context.Context,error) {
 if ctx==nil || identity==nil || identity.ProjectID()=="" || identity.ProjectID()!=stream.Tenant ||
    stream.Org=="" || stream.Application=="" || stream.Stream=="" || group=="" {
  return nil,errors.New("invalid project stream binding")
 }
 resource:=fmt.Sprintf("project/%s/skymill/%s/%s/%s/%s/%s",identity.ProjectID(),stream.Org,stream.Tenant,stream.Application,stream.Stream,group)
 actions:=make(map[skymill.Action]bool)
 for _,action:=range []skymill.Action{skymill.ActionPublish,skymill.ActionConsume,skymill.ActionSettle,skymill.ActionInspect} {
  if identity.Authorize(string(action),resource)==nil {actions[action]=true}
 }
 if len(actions)==0 {return nil,errors.New("project stream policy denies all actions")}
 return WithGrant(ctx,Grant{Binding:stream,ConsumerGroup:group,Actions:actions,Principal:identity.ProjectID()}),nil
}
