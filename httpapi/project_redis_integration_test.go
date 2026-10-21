package httpapi

import (
 "bytes"
 "context"
 "encoding/json"
 "fmt"
 "net/http"
 "net/http/httptest"
 "os"
 "testing"
 "time"

 "github.com/redis/go-redis/v9"
 "github.com/xd-dash/skymill"
)

// Both HTTP servers share one Redis instance but have distinct stream bindings.
func TestRedisHTTPProjectIsolation(t *testing.T) {
 url:=os.Getenv("SKYMILL_TEST_REDIS_URL")
 if url=="" {t.Skip("SKYMILL_TEST_REDIS_URL is required")}
 opts,err:=redis.ParseURL(url);if err!=nil {t.Fatal(err)}
 client:=redis.NewClient(opts);defer client.Close()
 if err:=client.Ping(context.Background()).Err();err!=nil {t.Fatal(err)}
 suffix:=fmt.Sprintf("%d",time.Now().UnixNano())
 type project struct {name string; stream *skymill.Stream; server *httptest.Server}
 projects:=make([]project,0,2)
 for _,name:=range []string{"alpha","beta"} {
  binding:=skymill.Binding{Org:"isolation",Tenant:name,Application:"httpapi",Stream:"events-"+suffix}
  stream,err:=skymill.New(skymill.Config{Client:client,Binding:binding,ConsumerGroup:"workers",Consumer:name,BlockTime:10*time.Millisecond})
  if err!=nil {t.Fatal(err)}
  auth:=func(r *http.Request)(context.Context,error){
   return ProjectGrant(r.Context(),policyFixture{project:r.Header.Get("X-Test-Project"),actions:map[string]bool{"publish":true,"consume":true,"inspect":true,"settle":true}},binding,"workers")
  }
  server:=httptest.NewServer(New(stream,auth).Handler())
  projects=append(projects,project{name,stream,server})
  t.Cleanup(func(){server.Close();_ = stream.Close()})
 }
 post:=func(p project,identity,path string,body []byte) int {
  t.Helper()
  req,err:=http.NewRequest(http.MethodPost,p.server.URL+path,bytes.NewReader(body));if err!=nil {t.Fatal(err)}
  req.Header.Set("X-Test-Project",identity)
  req.Header.Set("Content-Type","application/json")
  response,err:=http.DefaultClient.Do(req);if err!=nil {t.Fatal(err)}
  defer response.Body.Close()
  return response.StatusCode
 }
 alpha,beta:=projects[0],projects[1]
 publish:=[]byte(`{"idempotency_key":"isolation-1","message_id":"isolation-1","payload_base64":"e30="}`)
 if code:=post(alpha,"alpha","/v1/messages",publish);code!=http.StatusOK {t.Fatalf("alpha publish=%d",code)}
 if code:=post(beta,"alpha","/v1/messages",publish);code<400 {t.Fatalf("cross-project publish accepted=%d",code)}
 if code:=post(beta,"alpha","/v1/deliveries/receive",[]byte(`{"limit":1}`));code<400 {t.Fatalf("cross-project receive accepted=%d",code)}
 if code:=post(beta,"alpha","/v1/deliveries/ack",[]byte(`{"provider_delivery_id":"1-0"}`));code<400 {t.Fatalf("cross-project ACK accepted=%d",code)}
 if code:=post(beta,"alpha","/v1/workflows/settle",[]byte(`{"id":"cross","state":"succeeded"}`));code<400 {t.Fatalf("cross-project settlement accepted=%d",code)}
 // A valid project beta request must not read alpha's stream. Check directly
 // without issuing an indefinitely blocking receive request.
 if status,err:=beta.stream.Status(context.Background());err!=nil {t.Fatal(err)} else if status.StreamLength!=0 {t.Fatalf("beta stream contaminated: %+v",status)}
 // Verify alpha's own message remains accessible after denied operations.
 if code:=post(alpha,"alpha","/v1/deliveries/receive",[]byte(`{"limit":1}`));code!=http.StatusOK {t.Fatalf("alpha receive=%d",code)}
 // Check that beta can independently publish to its own binding.
 if code:=post(beta,"beta","/v1/messages",publish);code!=http.StatusOK {t.Fatalf("beta publish=%d",code)}
 _=json.Valid
}
