package skymill

import (
	"strings"
	"testing"
)

func TestRedisBindingKeysShareClusterSlotTag(t *testing.T) {
	b:=Binding{Org:"o",Tenant:"t",Application:"a",Stream:"work"}
	keys:=[]string{b.redisStreamKey(),b.redisAuxStreamKey("dead"),b.idempotencyKey("source"),b.correlationKey("workflow")}
	tag:="{"+b.redisSlotTag()+"}"
	for _,key:=range keys{if !strings.Contains(key,tag){t.Fatalf("key %q does not contain slot tag %q",key,tag)}}
}

func TestRedisBindingScopeChangesSlot(t *testing.T) {
	a:=Binding{Org:"o",Tenant:"one",Application:"a",Stream:"work"}
	b:=Binding{Org:"o",Tenant:"two",Application:"a",Stream:"work"}
	if a.redisSlotTag()==b.redisSlotTag(){t.Fatal("different authority scopes share slot tag")}
}
