package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/xd-dash/skymill"
)

type Authenticator func(*http.Request) (context.Context, error)

type Server struct {
	Stream       *skymill.Stream
	Authenticate Authenticator

	once sync.Once
	sub  <-chan *message.Message
	err  error

	mu      sync.Mutex
	pending map[string]*message.Message
}

func New(stream *skymill.Stream, authenticate Authenticator) *Server {
	return &Server{Stream: stream, Authenticate: authenticate, pending: make(map[string]*message.Message)}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/messages", s.publish)
	mux.HandleFunc("POST /v1/deliveries/receive", s.receive)
	mux.HandleFunc("POST /v1/deliveries/ack", s.ack)
	mux.HandleFunc("POST /v1/deliveries/nack", s.nack)
	mux.HandleFunc("POST /v1/workflows/correlate", s.correlate)
	mux.HandleFunc("POST /v1/workflows/settle", s.settle)
	mux.HandleFunc("GET /v1/status", s.status)
	return mux
}

func (s *Server) authorize(r *http.Request) (context.Context, error) {
	if s.Authenticate == nil { return r.Context(), nil }
	ctx, err := s.Authenticate(r)
	if err != nil { return nil, err }
	return ctx, nil
}

func (s *Server) ensureSubscriber(ctx context.Context) error {
	s.once.Do(func() { s.sub, s.err = s.Stream.Subscribe(context.Background()) })
	return s.err
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	ctx, err := s.authorize(r)
	if err != nil { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
	r = r.WithContext(ctx)
	if err := requireGrant(r, skymill.ActionPublish, s.Stream.Binding(), s.Stream.ConsumerGroup()); err != nil { http.Error(w, "forbidden", http.StatusForbidden); return }
	var req struct {
		IdempotencyKey string            `json:"idempotency_key"`
		MessageID      string            `json:"message_id"`
		Metadata       map[string]string `json:"metadata"`
		PayloadBase64  string            `json:"payload_base64"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.IdempotencyKey == "" {
		http.Error(w, "invalid request", http.StatusBadRequest); return
	}
	payload, err := base64.StdEncoding.DecodeString(req.PayloadBase64)
	if err != nil { http.Error(w, "invalid payload_base64", http.StatusBadRequest); return }
	if req.MessageID == "" { req.MessageID = req.IdempotencyKey }
	msg := message.NewMessage(req.MessageID, payload)
	for k, v := range req.Metadata { msg.Metadata.Set(k, v) }
	result, err := s.Stream.PublishOnce(ctx, req.IdempotencyKey, msg)
	if err != nil { http.Error(w, err.Error(), http.StatusServiceUnavailable); return }
	writeJSON(w, map[string]any{
		"message_id": req.MessageID, "stream_entry_id": result.StreamEntryID, "duplicate": result.Duplicate,
	})
}

func (s *Server) receive(w http.ResponseWriter, r *http.Request) {
	ctx, err := s.authorize(r)
	if err != nil { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
	r = r.WithContext(ctx)
	if err := requireGrant(r, skymill.ActionConsume, s.Stream.Binding(), s.Stream.ConsumerGroup()); err != nil { http.Error(w, "forbidden", http.StatusForbidden); return }
	var req struct { Limit int `json:"limit"` }
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Limit <= 0 { req.Limit = 25 }
	if req.Limit > 100 { req.Limit = 100 }
	if err := s.ensureSubscriber(ctx); err != nil { http.Error(w, err.Error(), http.StatusServiceUnavailable); return }

	out := make([]map[string]any, 0, req.Limit)
	for len(out) < req.Limit {
		var msg *message.Message
		if len(out) == 0 {
			select { case msg = <-s.sub: case <-ctx.Done(): writeJSON(w, map[string]any{"deliveries": out}); return }
		} else {
			select { case msg = <-s.sub: default: writeJSON(w, map[string]any{"deliveries": out}); return }
		}
		if msg == nil { break }
		token, err := randomToken()
		if err != nil { msg.Nack(); http.Error(w, err.Error(), http.StatusInternalServerError); return }
		s.mu.Lock(); s.pending[token] = msg; s.mu.Unlock()
		md := map[string]string{}
		for k, v := range msg.Metadata { md[k] = v }
		out = append(out, map[string]any{
			"token": token,
			"message": map[string]any{
				"id": msg.UUID, "metadata": md,
				"payload_base64": base64.StdEncoding.EncodeToString(msg.Payload),
			},
		})
	}
	writeJSON(w, map[string]any{"deliveries": out})
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request) { s.finish(w, r, true) }
func (s *Server) nack(w http.ResponseWriter, r *http.Request) { s.finish(w, r, false) }

func (s *Server) finish(w http.ResponseWriter, r *http.Request, ack bool) {
	ctx, err := s.authorize(r)
	if err != nil { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
	r = r.WithContext(ctx)
	if err := requireGrant(r, skymill.ActionConsume, s.Stream.Binding(), s.Stream.ConsumerGroup()); err != nil { http.Error(w, "forbidden", http.StatusForbidden); return }
	var req struct { Token string `json:"token"` }
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.Token == "" { http.Error(w, "invalid request", http.StatusBadRequest); return }
	s.mu.Lock(); msg := s.pending[req.Token]; if msg != nil { delete(s.pending, req.Token) }; s.mu.Unlock()
	if msg == nil { http.Error(w, "unknown delivery token", http.StatusNotFound); return }
	if ack {
		msg.Ack()
		s.Stream.ObserveAck(r.Context(), true)
		s.Stream.EmitCompletionHint(r.Context(), skymill.HintAcked, msg.UUID, "")
	} else {
		msg.Nack()
		s.Stream.ObserveAck(r.Context(), false)
		s.Stream.EmitCompletionHint(r.Context(), skymill.HintNacked, msg.UUID, "")
	}
	writeJSON(w, map[string]any{"ok": true})
}

func randomToken() (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil { return "", err }
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("content-type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil && !errors.Is(err, context.Canceled) { return }
}


func (s *Server) correlate(w http.ResponseWriter, r *http.Request) {
	ctx, err := s.authorize(r); if err != nil { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
	r = r.WithContext(ctx)
	if err := requireGrant(r, skymill.ActionSettle, s.Stream.Binding(), s.Stream.ConsumerGroup()); err != nil { http.Error(w, "forbidden", http.StatusForbidden); return }
	var req struct {
		ID string `json:"id"`
		MessageID string `json:"message_id"`
		StreamEntryID string `json:"stream_entry_id"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.ID == "" { http.Error(w, "invalid request", http.StatusBadRequest); return }
	err = s.Stream.Correlate(ctx, skymill.Correlation{ID:req.ID, MessageID:req.MessageID, StreamEntryID:req.StreamEntryID, ConsumerGroup:s.Stream.ConsumerGroup()})
	if err != nil { http.Error(w, err.Error(), http.StatusConflict); return }
	writeJSON(w, map[string]any{"ok":true})
}

func (s *Server) settle(w http.ResponseWriter, r *http.Request) {
	ctx, err := s.authorize(r); if err != nil { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
	r = r.WithContext(ctx)
	if err := requireGrant(r, skymill.ActionSettle, s.Stream.Binding(), s.Stream.ConsumerGroup()); err != nil { http.Error(w, "forbidden", http.StatusForbidden); return }
	var req struct { ID string `json:"id"`; State skymill.WorkflowState `json:"state"`; ResultRef string `json:"result_ref"`; Error string `json:"error"` }
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.ID == "" { http.Error(w, "invalid request", http.StatusBadRequest); return }
	result, err := s.Stream.SettleAndAck(ctx, req.ID, req.State, req.ResultRef, req.Error)
	if err != nil { http.Error(w, err.Error(), http.StatusConflict); return }
	writeJSON(w, result)
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	ctx, err := s.authorize(r); if err != nil { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
	r = r.WithContext(ctx)
	if err := requireGrant(r, skymill.ActionInspect, s.Stream.Binding(), s.Stream.ConsumerGroup()); err != nil { http.Error(w, "forbidden", http.StatusForbidden); return }
	status, err := s.Stream.Status(ctx)
	if err != nil { http.Error(w, err.Error(), http.StatusServiceUnavailable); return }
	writeJSON(w, status)
}
