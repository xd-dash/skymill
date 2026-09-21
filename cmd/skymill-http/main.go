package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/skymill"
	"github.com/xd-dash/skymill/httpapi"
)

func main() {
	addr := flag.String("addr", env("SKYMILL_ADDR", "127.0.0.1:3200"), "HTTP listen address")
	flag.Parse()

	db, err := strconv.Atoi(env("REDIS_DB", "2"))
	if err != nil { log.Fatalf("REDIS_DB: %v", err) }
	client := redis.NewClient(&redis.Options{
		Addr: env("REDIS_ADDR", "127.0.0.1:6379"),
		Username: os.Getenv("REDIS_USERNAME"),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB: db,
	})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := client.Ping(ctx).Err(); err != nil { log.Fatalf("redis: %v", err) }

	stream, err := skymill.New(skymill.Config{
		Client: client,
		Binding: skymill.Binding{
			Org: env("SKYMILL_ORG", "xd-dash"),
			Tenant: env("SKYMILL_TENANT", "probot-runtime"),
			Application: env("SKYMILL_APPLICATION", "github-webhooks"),
			Stream: env("SKYMILL_STREAM", "github.webhooks"),
		},
		ConsumerGroup: env("SKYMILL_CONSUMER_GROUP", "probot-runtime"),
		Consumer: os.Getenv("SKYMILL_CONSUMER"),
	})
	if err != nil { log.Fatal(err) }
	defer stream.Close()
	defer client.Close()

	server := &http.Server{Addr: *addr, Handler: httpapi.New(stream, nil).Handler()}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("skymill-http listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) { log.Fatal(err) }
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" { return value }
	return fallback
}
