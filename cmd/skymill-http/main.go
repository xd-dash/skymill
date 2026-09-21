package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/skymill"
	"github.com/xd-dash/skymill/httpapi"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:3200", "HTTP listen address")
	flag.Parse()
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" { redisURL = "redis://127.0.0.1:6379/0" }
	opts, err := redis.ParseURL(redisURL)
	if err != nil { log.Fatal(err) }
	client := redis.NewClient(opts)
	defer client.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil { log.Fatal(err) }
	streamName := env("SKYMILL_STREAM", "github.webhooks")
	group := env("SKYMILL_CONSUMER_GROUP", "probot-runtime")
	stream, err := skymill.New(skymill.Config{
		Client: client,
		Binding: skymill.Binding{Org: env("SKYMILL_ORG", "local"), Tenant: env("SKYMILL_TENANT", "local"), Application: env("SKYMILL_APPLICATION", "probot-runtime"), Stream: streamName},
		ConsumerGroup: group,
		Consumer: env("SKYMILL_CONSUMER", "skymill-http"),
	})
	if err != nil { log.Fatal(err) }
	defer stream.Close()
	server := &http.Server{Addr: *addr, Handler: httpapi.New(stream, nil).Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); shutdown, done := context.WithTimeout(context.Background(), 5*time.Second); defer done(); _ = server.Shutdown(shutdown) }()
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed { log.Fatal(err) }
}
func env(key, fallback string) string { if value := os.Getenv(key); value != "" { return value }; return fallback }
