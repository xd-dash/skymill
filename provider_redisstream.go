package skymill

import (
 "context"
 "errors"
 "time"
 "sync"
 "github.com/ThreeDotsLabs/watermill"
 "github.com/ThreeDotsLabs/watermill/message"
 "github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
 "github.com/redis/go-redis/v9"
)
type RedisStreamProviderConfig struct { Client redis.UniversalClient; Binding Binding; ConsumerGroup,Consumer string; MaxLen int64; NackResendSleep,BlockTime,ClaimInterval,MaxIdleTime,ConsumerTimeout time.Duration; ClaimBatchSize int64; Logger watermill.LoggerAdapter }
type redisStreamProvider struct { client redis.UniversalClient; binding Binding; group string; marshaller redisstream.Marshaller; publisher *redisstream.Publisher; claimInterval,blockTime,maxIdle time.Duration; claimBatch int64; cancel context.CancelFunc; wg sync.WaitGroup }
func newRedisStreamProvider(c RedisStreamProviderConfig)(*redisStreamProvider,error){
 l:=c.Logger;if l==nil{l=watermill.NopLogger{}}
 m:=redisstream.DefaultMarshallerUnmarshaller{};ml:=map[string]int64{};if c.MaxLen>0{ml[c.Binding.Stream]=c.MaxLen}
 pub,e:=redisstream.NewPublisher(redisstream.PublisherConfig{Client:c.Client,Marshaller:m,Maxlens:ml},l);if e!=nil{return nil,e}
 claim:=c.ClaimInterval;if claim==0{claim=5*time.Second};block:=c.BlockTime;if block==0{block=100*time.Millisecond};idle:=c.MaxIdleTime;if idle==0{idle=60*time.Second};batch:=c.ClaimBatchSize;if batch==0{batch=100}
 return &redisStreamProvider{client:c.Client,binding:c.Binding,group:c.ConsumerGroup,marshaller:m,publisher:pub,claimInterval:claim,blockTime:block,maxIdle:idle,claimBatch:batch},nil
}
func(p *redisStreamProvider)Publish(ctx context.Context,ms ...*message.Message)error{for _,m:=range ms{m.SetContext(ctx)};return p.publisher.Publish(p.binding.Stream,ms...)}
func(p *redisStreamProvider)PublishOnce(ctx context.Context,key string,msg *message.Message,r RetentionPolicy)(PublishResult,error){v,e:=p.marshaller.Marshal(p.binding.Stream,msg);if e!=nil{return PublishResult{},e};u,_:=v[redisstream.UUIDHeaderKey].(string);m,_:=v["metadata"].([]byte);b,_:=v["payload"].([]byte);script:="local e=redis.call('GET',KEYS[1]); if e then return {e,'1'} end; local id=redis.call('XADD',KEYS[2],'*','_watermill_message_uuid',ARGV[1],'metadata',ARGV[2],'payload',ARGV[3]); if tonumber(ARGV[4])>0 then redis.call('SET',KEYS[1],id,'PX',ARGV[4]) else redis.call('SET',KEYS[1],id) end; return {id,'0'}";x,e:=p.client.Eval(ctx,script,[]string{p.binding.idempotencyKey(key),p.binding.Stream},u,m,b,r.IdempotencyTTL.Milliseconds()).Slice();if e!=nil{return PublishResult{},e};if len(x)!=2{return PublishResult{},errors.New("skymill: invalid publish-once result")};id,_:=x[0].(string);d,_:=x[1].(string);return PublishResult{ProviderDeliveryID:id,Duplicate:d=="1"},nil}
func(p *redisStreamProvider)Subscribe(ctx context.Context)(<-chan Delivery,error){
 if p.group==""{return nil,errors.New("skymill: durable subscription requires a consumer group")}
 if e:=p.client.XGroupCreateMkStream(ctx,p.binding.Stream,p.group,"0").Err();e!=nil&&e.Error()!="BUSYGROUP Consumer Group name already exists"{return nil,e}
 ctx,cancel:=context.WithCancel(ctx);p.cancel=cancel;out:=make(chan Delivery);p.wg.Add(1)
 go p.consume(ctx,out)
 return out,nil
}

func(p *redisStreamProvider)consume(ctx context.Context,out chan<- Delivery){
 defer p.wg.Done();defer close(out);ticker:=time.NewTicker(p.claimInterval);defer ticker.Stop()
 for{
  select{case<-ctx.Done():return;case<-ticker.C:p.claim(ctx,out);default:}
  xs,e:=p.client.XReadGroup(ctx,&redis.XReadGroupArgs{Group:p.group,Consumer:p.consumer(),Streams:[]string{p.binding.Stream,">"},Count:1,Block:p.blockTime}).Result()
  if e==redis.Nil{continue};if e!=nil{if ctx.Err()!=nil{return};continue}
  for _,stream:=range xs{for _,xm:=range stream.Messages{if !p.send(ctx,out,xm){return}}}
 }
}
func(p *redisStreamProvider)consumer()string{return "skymill"}
func(p *redisStreamProvider)claim(ctx context.Context,out chan<- Delivery){
 rows,e:=p.client.XPendingExt(ctx,&redis.XPendingExtArgs{Stream:p.binding.Stream,Group:p.group,Idle:p.maxIdle,Start:"-",End:"+",Count:p.claimBatch}).Result();if e!=nil{return}
 for _,row:=range rows{xms,e:=p.client.XClaim(ctx,&redis.XClaimArgs{Stream:p.binding.Stream,Group:p.group,Consumer:p.consumer(),MinIdle:p.maxIdle,Messages:[]string{row.ID}}).Result();if e!=nil{continue};for _,xm:=range xms{if !p.send(ctx,out,xm){return}}}
}
func(p *redisStreamProvider)send(ctx context.Context,out chan<- Delivery,xm redis.XMessage)bool{
 msg,e:=p.marshaller.(redisstream.Unmarshaller).Unmarshal(xm.Values);if e!=nil{return true}
 d,e:=p.DeliveryState(ctx,xm.ID);if e!=nil{return true};d.Message=msg
 select{case out<-d:return true;case<-ctx.Done():return false}
}
func(p *redisStreamProvider)DeliveryState(ctx context.Context,id string)(Delivery,error){x,e:=p.client.XPendingExt(ctx,&redis.XPendingExtArgs{Stream:p.binding.Stream,Group:p.group,Start:id,End:id,Count:1}).Result();if e!=nil{return Delivery{},e};if len(x)==0||x[0].ID!=id{return Delivery{},errors.New("skymill: delivery is not pending")};return Delivery{ProviderDeliveryID:x[0].ID,ConsumerGroup:p.group,Consumer:x[0].Consumer,Attempt:x[0].RetryCount,Idle:x[0].Idle},nil}
func(p *redisStreamProvider)DeadLetter(ctx context.Context,d Delivery,msg *message.Message,stream string)error{v,e:=p.marshaller.Marshal(stream,msg);if e!=nil{return e};u,_:=v[redisstream.UUIDHeaderKey].(string);m,_:=v["metadata"].([]byte);b,_:=v["payload"].([]byte);script:="local e=redis.call('GET',KEYS[1]);if e then return e end;local id=redis.call('XADD',KEYS[2],'*','_watermill_message_uuid',ARGV[1],'metadata',ARGV[2],'payload',ARGV[3]);redis.call('SET',KEYS[1],id);return id";_,e=p.client.Eval(ctx,script,[]string{p.binding.idempotencyKey("dlq:"+d.ConsumerGroup+":"+d.ProviderDeliveryID),stream},u,m,b).Result();return e}
func(p *redisStreamProvider)Ack(ctx context.Context,g,id string)(bool,error){n,e:=p.client.XAck(ctx,p.binding.Stream,g,id).Result();return n>0,e}
func(p *redisStreamProvider)Status(ctx context.Context)(ProviderStatus,error){var s ProviderStatus;n,e:=p.client.XLen(ctx,p.binding.Stream).Result();if e!=nil&&e!=redis.Nil{return s,e};s.StreamLength=n;if p.group==""{return s,nil};x,e:=p.client.XPending(ctx,p.binding.Stream,p.group).Result();if e!=nil&&e!=redis.Nil{return s,e};if x!=nil{s.Pending=PendingStats{Count:x.Count,LowestID:x.Lower,HighestID:x.Higher,Consumers:int64(len(x.Consumers))}};cs,e:=p.client.XInfoConsumers(ctx,p.binding.Stream,p.group).Result();if e!=nil&&e!=redis.Nil{return s,e};s.Consumers=int64(len(cs));if s.Pending.Count>0{x,e:=p.client.XPendingExt(ctx,&redis.XPendingExtArgs{Stream:p.binding.Stream,Group:p.group,Start:"-",End:"+",Count:1}).Result();if e!=nil&&e!=redis.Nil{return s,e};if len(x)>0{s.OldestPendingIdle=x[0].Idle}};return s,nil}
func(p *redisStreamProvider)Close()error{if p.cancel!=nil{p.cancel()};p.wg.Wait();return p.publisher.Close()};if e:=p.publisher.Close();e!=nil&&x==nil{x=e};return x}

func(p *redisStreamProvider)CreateCorrelation(ctx context.Context,b Binding,c Correlation,w WorkflowPolicy)(bool,error){now:=time.Now().UTC();key:=b.correlationKey(c.ID);script:="if redis.call('EXISTS',KEYS[1])==1 then return 0 end;redis.call('HSET',KEYS[1],'id',ARGV[1],'message_id',ARGV[2],'stream_entry_id',ARGV[3],'consumer_group',ARGV[4],'state','pending','created_at',ARGV[5]);if tonumber(ARGV[6])>0 then redis.call('PEXPIRE',KEYS[1],ARGV[6]) end;return 1";n,e:=p.client.Eval(ctx,script,[]string{key},c.ID,c.MessageID,c.ProviderDeliveryID,c.ConsumerGroup,now.Format(time.RFC3339Nano),w.CorrelationTTL.Milliseconds()).Int();return n==1,e}
func(p *redisStreamProvider)GetCorrelation(ctx context.Context,b Binding,id string)(Correlation,error){x,e:=p.client.HGetAll(ctx,b.correlationKey(id)).Result();if e!=nil{return Correlation{},e};if len(x)==0{return Correlation{},nil};c:=Correlation{ID:x["id"],MessageID:x["message_id"],ProviderDeliveryID:x["stream_entry_id"],ConsumerGroup:x["consumer_group"],State:WorkflowState(x["state"]),ResultRef:x["result_ref"],Error:x["error"]};c.CreatedAt,_=time.Parse(time.RFC3339Nano,x["created_at"]);if t,e:=time.Parse(time.RFC3339Nano,x["settled_at"]);e==nil{c.SettledAt=&t};return c,nil}
func(p *redisStreamProvider)SettleCorrelation(ctx context.Context,b Binding,id string,state WorkflowState,result,detail string)(bool,error){now:=time.Now().UTC();script:="if redis.call('EXISTS',KEYS[1])==0 then return -1 end;if redis.call('HGET',KEYS[1],'state')~='pending' then return 0 end;redis.call('HSET',KEYS[1],'state',ARGV[1],'result_ref',ARGV[2],'error',ARGV[3],'settled_at',ARGV[4]);return 1";n,e:=p.client.Eval(ctx,script,[]string{b.correlationKey(id)},string(state),result,detail,now.Format(time.RFC3339Nano)).Int();if e!=nil{return false,e};if n<0{return false,errors.New("skymill: unknown correlation")};return n==1,nil}

func(p *redisStreamProvider)SettleAndAck(ctx context.Context,b Binding,id string,state WorkflowState,result,detail string)(Correlation,bool,bool,error){
	key:=b.correlationKey(id);now:=time.Now().UTC()
	script:="if redis.call('EXISTS',KEYS[1])==0 then return {-1,0} end;local current=redis.call('HGET',KEYS[1],'state');local changed=0;if current=='pending' then redis.call('HSET',KEYS[1],'state',ARGV[1],'result_ref',ARGV[2],'error',ARGV[3],'settled_at',ARGV[4]);changed=1 end;local group=redis.call('HGET',KEYS[1],'consumer_group');local delivery=redis.call('HGET',KEYS[1],'stream_entry_id');if not group or not delivery or group=='' or delivery=='' then return {changed,-1} end;local acked=redis.call('XACK',KEYS[2],group,delivery);return {changed,acked}"
	raw,e:=p.client.Eval(ctx,script,[]string{key,p.binding.Stream},string(state),result,detail,now.Format(time.RFC3339Nano)).Slice();if e!=nil{return Correlation{},false,false,e};if len(raw)!=2{return Correlation{},false,false,errors.New("skymill: invalid settle-and-ack result")};changed,_:=raw[0].(int64);acked,_:=raw[1].(int64);if changed<0{return Correlation{},false,false,errors.New("skymill: unknown correlation")};if acked<0{return Correlation{},false,false,errors.New("skymill: correlation is not ack-capable")};corr,e:=p.GetCorrelation(ctx,b,id);return corr,changed==1,acked>0,e
}
