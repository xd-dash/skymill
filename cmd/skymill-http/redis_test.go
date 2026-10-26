package main

import "testing"

func TestRedisOptions(t *testing.T) {
	for _, key := range []string{"REDIS_URL", "FATLINE_REDIS_URL", "REDIS_ADDR", "REDIS_DB", "REDIS_USERNAME", "REDIS_PASSWORD"} {
		t.Setenv(key, "")
	}
	t.Setenv("FATLINE_REDIS_URL", "rediss://worker:secret@redis.example:6380/5")
	opts, err := redisOptions()
	if err != nil { t.Fatal(err) }
	if opts.Addr != "redis.example:6380" || opts.DB != 5 || opts.Username != "worker" || opts.Password != "secret" || opts.TLSConfig == nil {
		t.Fatal("shared endpoint did not preserve address, database, credentials, and TLS")
	}
	t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/1")
	t.Setenv("REDIS_DB", "0")
	opts, err = redisOptions()
	if err != nil || opts.DB != 0 || opts.TLSConfig != nil { t.Fatal("explicit endpoint/database override failed") }
	t.Setenv("REDIS_DB", "-1")
	if _, err := redisOptions(); err == nil { t.Fatal("negative database accepted") }
	t.Setenv("REDIS_DB", "")
	t.Setenv("REDIS_URL", "redis://worker:secret@redis.example/bad")
	if _, err := redisOptions(); err == nil || err.Error() != "invalid Redis URL" { t.Fatal("URL error must not disclose credentials") }
}
