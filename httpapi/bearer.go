package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

func NewBearerAuthenticator(token []byte, grant Grant) (Authenticator, error) {
	if len(token) < 32 || len(token) > 4096 {
		return nil, errors.New("HTTP bearer credential must be 32-4096 bytes")
	}
	if strings.TrimSpace(string(token)) != string(token) {
		return nil, errors.New("HTTP bearer credential has surrounding whitespace")
	}
	expected := sha256.Sum256(token)
	return func(r *http.Request) (context.Context, error) {
		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			return nil, errors.New("missing bearer credential")
		}
		supplied := strings.TrimPrefix(header, "Bearer ")
		if len(supplied) < 32 || len(supplied) > 4096 || strings.ContainsAny(supplied, " \t\r\n") {
			return nil, errors.New("invalid bearer credential")
		}
		actual := sha256.Sum256([]byte(supplied))
		if subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
			return nil, errors.New("invalid bearer credential")
		}
		return WithGrant(r.Context(), grant), nil
	}, nil
}
