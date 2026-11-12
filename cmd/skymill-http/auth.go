package main

import (
    "context"
    "crypto/subtle"
    "errors"
    "net/http"
    "os"
    "strings"

    "github.com/xd-dash/skymill"
    "github.com/xd-dash/skymill/httpapi"
)

func tokenAuthenticator(path string, binding skymill.Binding, group string) (httpapi.Authenticator, error) {
    if path == "" {
        return nil, errors.New("SKYMILL_TOKEN_FILE is required")
    }
    raw, err := os.ReadFile(path)
    if err != nil { return nil, err }
    secret := strings.TrimRight(string(raw), "\r\n")
    if len(secret) < 32 || strings.TrimSpace(secret) != secret {
        return nil, errors.New("invalid Skymill service token")
    }
    token := []byte("Bearer " + secret)
    grant := httpapi.Grant{
        Binding: binding,
        ConsumerGroup: group,
        Principal: "probot-runtime",
        Actions: map[skymill.Action]bool{
            skymill.ActionPublish: true,
            skymill.ActionConsume: true,
            skymill.ActionSettle: true,
            skymill.ActionInspect: true,
        },
    }
    return func(request *http.Request) (context.Context, error) {
        supplied := []byte(request.Header.Get("Authorization"))
        if subtle.ConstantTimeCompare(supplied, token) != 1 {
            return nil, errors.New("unauthorized")
        }
        return httpapi.WithGrant(request.Context(), grant), nil
    }, nil
}
