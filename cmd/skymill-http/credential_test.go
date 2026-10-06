package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRedisCredentialFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(file, []byte("fixture-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REDIS_PASSWORD_FILE", file)
	t.Setenv("REDIS_PASSWORD", "")
	opts, err := redisOptions()
	if err != nil || opts.Password != "fixture-secret" {
		t.Fatal("credential file was not applied")
	}
	t.Setenv("REDIS_PASSWORD", "ambiguous")
	if _, err := redisOptions(); err == nil {
		t.Fatal("ambiguous credential input accepted")
	}
}
