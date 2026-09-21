package skymill

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
)

func integrationRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr:=os.Getenv("SKYMILL_TEST_REDIS_ADDR")
	if addr=="" { t.Skip("SKYMILL_TEST_REDIS_ADDR not set") }
	c:=redis.NewClient(&redis.Options{Addr:addr})
	t.Cleanup(func(){ _=c.Close() })
	if err:=c.FlushDB(context.Background()).Err();err!=nil{t.Fatal(err)}
	return c
}

func integrationProvider(t *testing.T,c *redis.Client,consumer string)*redisStreamProvider{
	t.Helper()
	p,err:=newRedisStreamProvider(RedisStreamProviderConfig{Client:c,Binding:Binding{Org:"o",Tenant:"t",Application:"a",Stream:"work"},ConsumerGroup:"workers",Consumer:consumer,MaxIdleTime:time.Millisecond})
	if err!=nil{t.Fatal(err)}
	return p
}

func TestRedisPublishOnceDuplicate(t *testing.T){
	ctx:=context.Background();c:=integrationRedis(t);p:=integrationProvider(t,c,"c1")
	first,err:=p.PublishOnce(ctx,"source-1",message.NewMessage("m1",[]byte("one")),RetentionPolicy{});if err!=nil{t.Fatal(err)}
	second,err:=p.PublishOnce(ctx,"source-1",message.NewMessage("m2",[]byte("two")),RetentionPolicy{});if err!=nil{t.Fatal(err)}
	if first.Duplicate||!second.Duplicate||first.ProviderDeliveryID!=second.ProviderDeliveryID{t.Fatalf("first=%#v second=%#v",first,second)}
	if n:=c.XLen(ctx,"work").Val();n!=1{t.Fatalf("stream len=%d",n)}
}

func makePending(t *testing.T,c *redis.Client,consumer string)(string,*message.Message){
	t.Helper();ctx:=context.Background()
	p:=integrationProvider(t,c,consumer)
	r,err:=p.PublishOnce(ctx,"source",message.NewMessage("m",[]byte("payload")),RetentionPolicy{});if err!=nil{t.Fatal(err)}
	if err:=c.XGroupCreateMkStream(ctx,"work","workers","0").Err();err!=nil && err.Error()!="BUSYGROUP Consumer Group name already exists"{t.Fatal(err)}
	x,err:=c.XReadGroup(ctx,&redis.XReadGroupArgs{Group:"workers",Consumer:consumer,Streams:[]string{"work",">"},Count:1}).Result();if err!=nil{t.Fatal(err)}
	if len(x)==0||len(x[0].Messages)==0{t.Fatal("no delivery")}
	return r.ProviderDeliveryID,message.NewMessage("m",[]byte("payload"))
}

func TestRedisPELAttemptSurvivesClaim(t *testing.T){
	ctx:=context.Background();c:=integrationRedis(t);p:=integrationProvider(t,c,"c1")
	id,_:=makePending(t,c,"c1")
	before,err:=p.DeliveryState(ctx,id);if err!=nil{t.Fatal(err)}
	time.Sleep(2*time.Millisecond)
	if _,err:=c.XClaim(ctx,&redis.XClaimArgs{Stream:"work",Group:"workers",Consumer:"c2",MinIdle:time.Millisecond,Messages:[]string{id}}).Result();err!=nil{t.Fatal(err)}
	after,err:=p.DeliveryState(ctx,id);if err!=nil{t.Fatal(err)}
	if after.Consumer!="c2"||after.Attempt<=before.Attempt{t.Fatalf("before=%#v after=%#v",before,after)}
}

func TestRedisDeadLetterFailureDoesNotAckSource(t *testing.T){
	ctx:=context.Background();c:=integrationRedis(t);p:=integrationProvider(t,c,"c1")
	id,msg:=makePending(t,c,"c1");d,err:=p.DeliveryState(ctx,id);if err!=nil{t.Fatal(err)}
	bad, cancel:=context.WithCancel(ctx);cancel()
	if err:=p.DeadLetter(bad,d,msg,"dlq");err==nil{t.Fatal("expected DLQ failure")}
	state,err:=p.DeliveryState(ctx,id);if err!=nil{t.Fatal(err)}
	if state.ProviderDeliveryID!=id{t.Fatalf("delivery disappeared: %#v",state)}
}

func TestRedisSettleAndAckIsRepeatableAfterConsumerDisappears(t *testing.T){
	ctx:=context.Background();c:=integrationRedis(t);p:=integrationProvider(t,c,"c1")
	id,_:=makePending(t,c,"c1")
	corr:=Correlation{ID:"workflow-1",MessageID:"m",ProviderDeliveryID:id,ConsumerGroup:"workers"}
	created,err:=p.CreateCorrelation(ctx,p.binding,corr,WorkflowPolicy{});if err!=nil||!created{t.Fatalf("created=%v err=%v",created,err)}
	// Simulate the original worker disappearing. PEL ownership may remain c1;
	// settlement must use durable correlation identity, not process memory.
	got,changed,acked,err:=p.SettleAndAck(ctx,p.binding,"workflow-1",WorkflowSucceeded,"result://1","")
	if err!=nil{t.Fatal(err)};if !changed||!acked||got.State!=WorkflowSucceeded{t.Fatalf("got=%#v changed=%v acked=%v",got,changed,acked)}
	got,changed,acked,err=p.SettleAndAck(ctx,p.binding,"workflow-1",WorkflowSucceeded,"result://1","")
	if err!=nil{t.Fatal(err)};if changed||acked||got.State!=WorkflowSucceeded{t.Fatalf("repeat got=%#v changed=%v acked=%v",got,changed,acked)}
	if pnd:=c.XPending(ctx,"work","workers").Val();pnd!=nil&&pnd.Count!=0{t.Fatalf("pending=%d",pnd.Count)}
}
