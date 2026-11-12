package main

import (
    "net/http"
    "os"
    "path/filepath"
    "testing"

    "github.com/xd-dash/skymill"
    "github.com/xd-dash/skymill/httpapi"
)

func TestTokenAuthenticatorRequiresValidRuntimeFile(t *testing.T) {
    binding := skymill.Binding{Org:"xd-dash", Tenant:"probot-runtime", Application:"github-webhooks", Stream:"github.webhooks"}
    if _, err := tokenAuthenticator("",binding,"probot-runtime"); err == nil {
        t.Fatal("missing runtime token accepted")
    }
    path := filepath.Join(t.TempDir(),"token")
    if err := os.WriteFile(path,[]byte("short\n"),0600); err != nil { t.Fatal(err) }
    if _, err := tokenAuthenticator(path,binding,"probot-runtime"); err == nil {
        t.Fatal("short runtime token accepted")
    }
    if err := os.WriteFile(path,[]byte("abcd1234abcd1234abcd1234abcd1234\n"),0600); err != nil { t.Fatal(err) }
    authenticate, err := tokenAuthenticator(path,binding,"probot-runtime")
    if err != nil { t.Fatal(err) }
    for _, test := range []struct {
        name,header string
        allowed bool
    }{
        {"absent","",false},
        {"invalid","Bearer invalid",false},
        {"wrong-scheme","Basic abcd1234abcd1234abcd1234abcd1234",false},
        {"valid","Bearer abcd1234abcd1234abcd1234abcd1234",true},
    } {
        t.Run(test.name,func(t *testing.T){
            request,_:=http.NewRequest(http.MethodPost,"http://skymill/v1/messages",nil)
            request.Header.Set("Authorization",test.header)
            ctx,err:=authenticate(request)
            if (err==nil)!=test.allowed { t.Fatalf("authorization = %v, expected allow=%v",err,test.allowed) }
            if test.allowed {
                grant,ok:=httpapi.GrantFromContext(ctx)
                if !ok || grant.Binding!=binding || grant.ConsumerGroup!="probot-runtime" ||
                    !grant.Actions[skymill.ActionSettle] || !grant.Actions[skymill.ActionPublish] {
                    t.Fatal("authenticated request missing exact durable delivery grant")
                }
            }
        })
    }
}
