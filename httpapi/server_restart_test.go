package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xd-dash/skymill"
)

func TestDeliveryCanBeAckedAfterHTTPServerRestart(t *testing.T) {
	redisURL := os.Getenv("SKYMILL_TEST_REDIS_URL")
	if redisURL == "" { t.Skip("SKYMILL_TEST_REDIS_URL is required") }
	opts, err := redis.ParseURL(redisURL)
	if err != nil { t.Fatal(err) }
	client := redis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil { t.Fatal(err) }

	suffix := time.Now().UTC().Format("20060102T150405.000000000")
	binding := skymill.Binding{Org:"test", Tenant:"test", Application:"httpapi", Stream:"restart-"+suffix}
	const group = "restart-test"

	newStream := func() *skymill.Stream {
		stream, err := skymill.New(skymill.Config{Client:client, Binding:binding, ConsumerGroup:group, Consumer:"httpapi-test", BlockTime:10*time.Millisecond})
		if err != nil { t.Fatal(err) }
		return stream
	}

	streamA := newStream()
	serverA := httptest.NewServer(New(streamA, nil).Handler())
	publishBody := []byte(`{"idempotency_key":"github-delivery-1","message_id":"github-delivery-1","payload_base64":"e30="}`)
	resp, err := http.Post(serverA.URL+"/v1/messages", "application/json", bytes.NewReader(publishBody))
	if err != nil { t.Fatal(err) }
	if resp.StatusCode != http.StatusOK { t.Fatalf("publish status = %d", resp.StatusCode) }
	_ = resp.Body.Close()

	resp, err = http.Post(serverA.URL+"/v1/deliveries/receive", "application/json", bytes.NewReader([]byte(`{"limit":1}`)))
	if err != nil { t.Fatal(err) }
	if resp.StatusCode != http.StatusOK { t.Fatalf("receive status = %d", resp.StatusCode) }
	var received struct { Deliveries []struct { ProviderDeliveryID string `json:"provider_delivery_id"` } `json:"deliveries"` }
	if err := json.NewDecoder(resp.Body).Decode(&received); err != nil { t.Fatal(err) }
	_ = resp.Body.Close()
	if len(received.Deliveries) != 1 || received.Deliveries[0].ProviderDeliveryID == "" { t.Fatalf("unexpected deliveries: %+v", received.Deliveries) }
	providerID := received.Deliveries[0].ProviderDeliveryID

	serverA.Close()
	if err := streamA.Close(); err != nil { t.Fatal(err) }

	streamB := newStream()
	t.Cleanup(func() { _ = streamB.Close() })
	serverB := httptest.NewServer(New(streamB, nil).Handler())
	t.Cleanup(serverB.Close)

	ackBody, _ := json.Marshal(map[string]string{"provider_delivery_id": providerID})
	resp, err = http.Post(serverB.URL+"/v1/deliveries/ack", "application/json", bytes.NewReader(ackBody))
	if err != nil { t.Fatal(err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { t.Fatalf("ack after restart status = %d", resp.StatusCode) }

	if _, err := streamB.DeliveryState(ctx, providerID); err == nil {
		t.Fatal("delivery remains pending after ACK")
	}
}
