package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/skymill"
	"github.com/xd-dash/skymill/httpapi"
)

func main() {
	addr := getenv("SKYMILL_LISTEN_ADDR", "127.0.0.1:8081")
	redisAddr := getenv("REDIS_ADDR", "127.0.0.1:6379")
	client := redis.NewClient(&redis.Options{Addr: redisAddr, Username: os.Getenv("REDIS_USERNAME"), Password: os.Getenv("REDIS_PASSWORD")})
	defer client.Close()
	stream, err := skymill.New(skymill.Config{
		Client: client,
		Binding: skymill.Binding{Org: getenv("SKYMILL_ORG", "xd-dash"), Tenant: getenv("SKYMILL_TENANT", "probot-runtime"), Application: getenv("SKYMILL_APPLICATION", "github-webhooks"), Stream: getenv("SKYMILL_STREAM", "github.webhooks")},
		ConsumerGroup: getenv("SKYMILL_CONSUMER_GROUP", "probot-runtime"),
		Consumer: os.Getenv("SKYMILL_CONSUMER"),
		BlockTime: time.Second,
	})
	if err != nil { log.Fatal(err) }
	defer stream.Close()
	log.Printf("skymill-http listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, httpapi.New(stream, nil).Handler()))
}
func getenv(k, fallback string) string { if v:=os.Getenv(k); v!="" { return v }; return fallback }
