package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xd-dash/skymill"
)

func TestBearerAuthenticator(t *testing.T) {
	grant := Grant{
		Principal: "probot-runtime",
		Actions: map[skymill.Action]bool{skymill.ActionPublish: true},
	}
	authenticate, err := NewBearerAuthenticator([]byte(strings.Repeat("a", 64)), grant)
	if err != nil { t.Fatal(err) }

	for _, tc := range []struct {
		name, header string
		authorized bool
	}{
		{"valid", "Bearer " + strings.Repeat("a", 64), true},
		{"missing", "", false},
		{"wrong", "Bearer " + strings.Repeat("b", 64), false},
		{"wrong_scheme", "Basic " + strings.Repeat("a", 64), false},
		{"short", "Bearer abc", false},
		{"extra", "Bearer " + strings.Repeat("a", 64) + " ", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			if tc.header != "" { req.Header.Set("Authorization", tc.header) }
			ctx, err := authenticate(req)
			if tc.authorized {
				if err != nil { t.Fatal(err) }
				received, ok := GrantFromContext(ctx)
				if !ok || received.Principal != grant.Principal || !received.Actions[skymill.ActionPublish] {
					t.Fatal("authenticated request missing intended grant")
				}
			} else if err == nil {
				t.Fatal("unauthorized request accepted")
			}
		})
	}
}

func TestBearerAuthenticatorRejectsInvalidConfiguration(t *testing.T) {
	for _, token := range [][]byte{nil, []byte("short"), []byte(strings.Repeat("x", 32) + "\n")} {
		if _, err := NewBearerAuthenticator(token, Grant{}); err == nil {
			t.Fatal("invalid bearer credential accepted")
		}
	}
}
