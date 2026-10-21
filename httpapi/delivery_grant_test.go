package httpapi

import (
    "net/http/httptest"
    "testing"

    "github.com/xd-dash/skymill"
)

func TestDeliveryGrantSeparation(t *testing.T) {
    binding := skymill.Binding{Org:"org",Tenant:"alpha",Application:"app",Stream:"events"}
    cases := []struct {
        name string
        actions map[skymill.Action]bool
        settle bool
        inspect bool
    }{
        {"consume-only",map[skymill.Action]bool{skymill.ActionConsume:true},false,false},
        {"settle-only",map[skymill.Action]bool{skymill.ActionSettle:true},true,false},
        {"inspect-only",map[skymill.Action]bool{skymill.ActionInspect:true},false,true},
        {"settle-and-inspect",map[skymill.Action]bool{skymill.ActionSettle:true,skymill.ActionInspect:true},true,true},
    }
    for _,tc := range cases {
        t.Run(tc.name,func(t *testing.T){
            r:=httptest.NewRequest("POST","/v1/deliveries/ack",nil)
            r=r.WithContext(WithGrant(r.Context(),Grant{Binding:binding,ConsumerGroup:"group",Actions:tc.actions,Principal:"alpha"}))
            if got:=requireGrant(r,skymill.ActionSettle,binding,"group")==nil;got!=tc.settle {t.Fatalf("settle grant=%v, want %v",got,tc.settle)}
            if got:=requireGrant(r,skymill.ActionInspect,binding,"group")==nil;got!=tc.inspect {t.Fatalf("inspect grant=%v, want %v",got,tc.inspect)}
        })
    }
}
