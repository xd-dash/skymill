package main

import (
	"fmt"
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
	opts, err := redisOptions()
	if err != nil { log.Fatal(err) }
	client := redis.NewClient(opts)
	defer client.Close()
	stream, err := skymill.New(skymill.Config{
		Client: client,
		Binding: skymill.Binding{Org: getenv("SKYMILL_ORG", "xd-dash"), Tenant: getenv("SKYMILL_TENANT", "probot-runtime"), Application: getenv("SKYMILL_APPLICATION", "github-webhooks"), Stream: getenv("SKYMILL_STREAM", "github.webhooks")},
		ConsumerGroup: getenv("SKYMILL_CONSUMER_GROUP", "probot-runtime"),
		Consumer: os.Getenv("SKYMILL_CONSUMER"),
		BlockTime: durationEnv("SKYMILL_BLOCK_TIME", time.Second),
		ClaimInterval: durationEnv("SKYMILL_CLAIM_INTERVAL", 0),
		ClaimBatchSize: int64Env("SKYMILL_CLAIM_BATCH_SIZE", 0),
		MaxIdleTime: durationEnv("SKYMILL_MAX_IDLE_TIME", 0),
		ConsumerTimeout: durationEnv("SKYMILL_CONSUMER_TIMEOUT", 0),
	})
	if err != nil { log.Fatal(err) }
	defer stream.Close()
	log.Printf("skymill-http listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, httpapi.New(stream, nil).Handler()))
}
func getenv(k, fallback string) string { if v:=os.Getenv(k); v!="" { return v }; return fallback }
func durationEnv(k string, fallback time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" { return fallback }
	d, err := time.ParseDuration(v)
	if err != nil { log.Fatalf("%s: %v", k, err) }
	return d
}
func int64Env(k string, fallback int64) int64 {
	v := os.Getenv(k)
	if v == "" { return fallback }
	var n int64
	if _, err := fmt.Sscan(v, &n); err != nil { log.Fatalf("%s: %v", k, err) }
	return n
}
