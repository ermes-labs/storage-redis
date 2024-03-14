package main_test

import (
	"context"
	"testing"

	"github.com/ermes-labs/api-go/api"
	"github.com/ermes-labs/spec-tests"
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

func TestSpec(t *testing.T) {
	cmd, free := env.New("node")
	defer free()
	// Print the time
	cmd.Set_current_node_key(context.Background(), "time")

	cmd.AcquireSession(context.Background(), "miao", api.NewAcquireSessionOptionsBuilder().Build())

	spec.RunTests(t, &env)
}
