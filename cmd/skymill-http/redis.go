package main

import (
	"errors"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func redisOptions() (*redis.Options, error) {
	opts := &redis.Options{Addr: getenv("REDIS_ADDR", "127.0.0.1:6379")}
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = os.Getenv("FATLINE_REDIS_URL")
	}
	if url != "" {
		var err error
		opts, err = redis.ParseURL(url)
		if err != nil {
			return nil, errors.New("invalid Redis URL")
		}
	}
	if db := os.Getenv("REDIS_DB"); db != "" {
		n, err := strconv.Atoi(db)
		if err != nil || n < 0 {
			return nil, errors.New("REDIS_DB must be a non-negative integer")
		}
		opts.DB = n
	}
	if username := os.Getenv("REDIS_USERNAME"); username != "" {
		opts.Username = username
	}
	if password := os.Getenv("REDIS_PASSWORD"); password != "" {
		opts.Password = password
	}
	return opts, nil
}
