package skymill

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

type fakeProvider struct {
	delivery Delivery
	status ProviderStatus
	acked bool
	deadLetterErr error
	correlation Correlation
	settled bool
}

func(f *fakeProvider)Publish(context.Context,...*message.Message)error{return nil}
func(f *fakeProvider)PublishOnce(context.Context,string,*message.Message,RetentionPolicy)(PublishResult,error){return PublishResult{ProviderDeliveryID:"p1"},nil}
func(f *fakeProvider)Subscribe(context.Context)(<-chan *message.Message,error){return make(chan *message.Message),nil}
func(f *fakeProvider)DeliveryState(context.Context,string)(Delivery,error){return f.delivery,nil}
func(f *fakeProvider)DeadLetter(context.Context,Delivery,*message.Message,string)error{return f.deadLetterErr}
func(f *fakeProvider)Ack(context.Context,string,string)(bool,error){f.acked=true;return true,nil}
func(f *fakeProvider)Status(context.Context)(ProviderStatus,error){return f.status,nil}
func(f *fakeProvider)CreateCorrelation(_ context.Context,_ Binding,c Correlation,_ WorkflowPolicy)(bool,error){if f.correlation.ID!=""{return false,nil};c.State=WorkflowPending;c.CreatedAt=time.Now();f.correlation=c;return true,nil}
func(f *fakeProvider)GetCorrelation(context.Context,Binding,string)(Correlation,error){return f.correlation,nil}
func(f *fakeProvider)SettleCorrelation(_ context.Context,_ Binding,_ string,state WorkflowState,result,detail string)(bool,error){if f.correlation.ID==""{return false,errors.New("unknown")};if f.correlation.State!=WorkflowPending{return false,nil};f.correlation.State=state;f.correlation.ResultRef=result;f.correlation.Error=detail;f.settled=true;return true,nil}
func(f *fakeProvider)Close()error{return nil}

func TestPrepareDeliveryUsesProviderAttemptAndDLQFailureDoesNotAck(t *testing.T){
	p:=&fakeProvider{delivery:Delivery{ProviderDeliveryID:"p1",ConsumerGroup:"g",Attempt:4},deadLetterErr:errors.New("dlq down")}
	s:=&Stream{binding:Binding{Stream:"s"},group:"g",provider:p,policy:Policy{Retry:RetryPolicy{MaxDeliveries:3,DeadLetterStream:"dlq"}}}
	ok,err:=s.PrepareDelivery(context.Background(),"p1",message.NewMessage("m",[]byte("x")))
	if err==nil||ok{t.Fatalf("ok=%v err=%v",ok,err)}
	if p.acked{t.Fatal("source must not ack when DLQ fails")}
}

func TestSettleAndAckSurvivesOriginalWorker(t *testing.T){
	p:=&fakeProvider{correlation:Correlation{ID:"w",MessageID:"m",ProviderDeliveryID:"p1",ConsumerGroup:"g",State:WorkflowPending}}
	s:=&Stream{binding:Binding{Stream:"s"},group:"g",provider:p}
	got,err:=s.SettleAndAck(context.Background(),"w",WorkflowSucceeded,"result","")
	if err!=nil{t.Fatal(err)}
	if !got.Acked||!p.acked||!p.settled{t.Fatalf("result=%+v provider=%+v",got,p)}
}

func TestDuplicateSettlementStillRetriesAck(t *testing.T){
	p:=&fakeProvider{correlation:Correlation{ID:"w",MessageID:"m",ProviderDeliveryID:"p1",ConsumerGroup:"g",State:WorkflowSucceeded}}
	s:=&Stream{binding:Binding{Stream:"s"},group:"g",provider:p}
	got,err:=s.SettleAndAck(context.Background(),"w",WorkflowSucceeded,"result","")
	if err!=nil{t.Fatal(err)}
	if !got.Acked||!p.acked{t.Fatal("terminal workflow must still attempt provider ack")}
}

func TestStatusIsProviderNeutral(t *testing.T){
	p:=&fakeProvider{status:ProviderStatus{StreamLength:10,Pending:PendingStats{Count:2},Consumers:1}}
	s:=&Stream{binding:Binding{Stream:"s"},group:"g",provider:p}
	got,err:=s.Status(context.Background());if err!=nil{t.Fatal(err)}
	if got.StreamLength!=10||got.Pending.Count!=2||got.Consumers!=1{t.Fatalf("%+v",got)}
}
