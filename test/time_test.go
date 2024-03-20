package main_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestGetTime(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,
	})
	// Request the time from Redis
	result, err := client.Time(context.Background()).Result()
	if err != nil {
		t.Errorf("Failed to get time from Redis: %v", err)
	}

	// Assert that the time is not null
	if result.IsZero() {
		t.Errorf("Received null time from Redis")
	}
}
