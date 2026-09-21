package skymill

import (
	"context"
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
)

type fakeProvider struct {
	delivery Delivery
	status ProviderStatus
	deadLetterErr error
	correlation Correlation
	settleChanged bool
	acked bool
}

func(f *fakeProvider)Publish(context.Context,...*message.Message)error{return nil}
func(f *fakeProvider)PublishOnce(context.Context,string,*message.Message,RetentionPolicy)(PublishResult,error){return PublishResult{},nil}
func(f *fakeProvider)Subscribe(context.Context)(<-chan *message.Message,error){return make(chan *message.Message),nil}
func(f *fakeProvider)DeliveryState(context.Context,string)(Delivery,error){return f.delivery,nil}
func(f *fakeProvider)DeadLetter(context.Context,Delivery,*message.Message,string)error{return f.deadLetterErr}
func(f *fakeProvider)Ack(context.Context,string,string)(bool,error){return f.acked,nil}
func(f *fakeProvider)Status(context.Context)(ProviderStatus,error){return f.status,nil}
func(f *fakeProvider)CreateCorrelation(context.Context,Binding,Correlation,WorkflowPolicy)(bool,error){return true,nil}
func(f *fakeProvider)GetCorrelation(context.Context,Binding,string)(Correlation,error){return f.correlation,nil}
func(f *fakeProvider)SettleCorrelation(context.Context,Binding,string,WorkflowState,string,string)(bool,error){return f.settleChanged,nil}
func(f *fakeProvider)SettleAndAck(context.Context,Binding,string,WorkflowState,string,string)(Correlation,bool,bool,error){return f.correlation,f.settleChanged,f.acked,nil}
func(f *fakeProvider)Close()error{return nil}

func TestPrepareDeliveryUsesProviderAttempt(t *testing.T){
	p:=&fakeProvider{delivery:Delivery{ProviderDeliveryID:"opaque",ConsumerGroup:"g",Attempt:2}}
	s:=&Stream{binding:Binding{Stream:"s"},group:"g",provider:p,policy:Policy{Retry:RetryPolicy{MaxDeliveries:2,DeadLetterStream:"dlq"}}}
	ok,err:=s.PrepareDelivery(context.Background(),"opaque",message.NewMessage("m",nil));if err!=nil||!ok{t.Fatalf("ok=%v err=%v",ok,err)}
	p.delivery.Attempt=3
	ok,err=s.PrepareDelivery(context.Background(),"opaque",message.NewMessage("m",nil));if err!=nil||ok{t.Fatalf("ok=%v err=%v",ok,err)}
}

func TestDeadLetterFailureLeavesDeliveryRetryable(t *testing.T){
	p:=&fakeProvider{delivery:Delivery{ProviderDeliveryID:"opaque",ConsumerGroup:"g",Attempt:3},deadLetterErr:errors.New("down")}
	s:=&Stream{binding:Binding{Stream:"s"},group:"g",provider:p,policy:Policy{Retry:RetryPolicy{MaxDeliveries:2,DeadLetterStream:"dlq"}}}
	ok,err:=s.PrepareDelivery(context.Background(),"opaque",message.NewMessage("m",nil));if err==nil||ok{t.Fatalf("ok=%v err=%v",ok,err)}
}

func TestSettleAndAckIsProviderAtomicBoundary(t *testing.T){
	c:=Correlation{ID:"w",MessageID:"m",ProviderDeliveryID:"opaque",ConsumerGroup:"g",State:WorkflowSucceeded}
	p:=&fakeProvider{correlation:c,settleChanged:true,acked:true}
	s:=&Stream{binding:Binding{Stream:"s"},group:"g",provider:p}
	r,err:=s.SettleAndAck(context.Background(),"w",WorkflowSucceeded,"result","");if err!=nil{t.Fatal(err)};if !r.Acked||r.Correlation.ID!="w"{t.Fatalf("%#v",r)}
}
