package redis_commands

import (
	"context"

	"github.com/ermes-labs/api-go/api"
	"github.com/redis/go-redis/v9"
)

// RedisCommands is a wrapper around the Redis client.
type RedisCommands struct {
	api.Commands
	client *redis.Client
}

// NewRedisCommands creates a new RedisCommands instance.
func NewRedisCommands(client *redis.Client) *RedisCommands {
	return &RedisCommands{
		client: client,
	}
}

func (c *RedisCommands) Set_current_node_key(ctx context.Context, nodeId string) error {
	return c.client.FCall(ctx, "set_current_node_key", []string{nodeId}).Err()
}
