package httpapi

import (
 "context"
 "errors"
 "testing"

 "github.com/xd-dash/skymill"
)
type policyFixture struct {project string; actions map[string]bool}
func (p policyFixture) ProjectID() string {return p.project}
func (p policyFixture) Authorize(action,resource string) error {
 expected:="project/"+p.project+"/skymill/org/"+p.project+"/app/events/group"
 if resource==expected && p.actions[action] {return nil}
 return errors.New("denied")
}
func TestProjectGrantIsolation(t *testing.T) {
 stream:=skymill.Binding{Org:"org",Tenant:"alpha",Application:"app",Stream:"events"}
 identity:=policyFixture{project:"alpha",actions:map[string]bool{"publish":true}}
 ctx,err:=ProjectGrant(context.Background(),identity,stream,"group")
 if err!=nil {t.Fatal(err)}
 grant,ok:=GrantFromContext(ctx)
 if !ok || !grant.Actions[skymill.ActionPublish] || grant.Actions[skymill.ActionConsume] {t.Fatalf("incorrect grant: %+v",grant)}
 if _,err:=ProjectGrant(context.Background(),policyFixture{project:"beta",actions:map[string]bool{"publish":true}},stream,"group");err==nil {t.Fatal("cross-project grant permitted")}
 if _,err:=ProjectGrant(context.Background(),policyFixture{project:"alpha"},stream,"group");err==nil {t.Fatal("empty policy permitted")}
 if _,err:=ProjectGrant(context.Background(),nil,stream,"group");err==nil {t.Fatal("missing authority permitted")}
}
