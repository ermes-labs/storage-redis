package main_test

import (
	"context"
	"os"
	"testing"

	"github.com/ermes-labs/spec-tests"
	redis_commands "github.com/ermes-labs/storage-redis/packages/go"
	"github.com/redis/go-redis/v9"
)

var env = Env{
	dbPool: make(chan int, 16),
}

func TestMain(m *testing.M) {
	// Populate the pool
	for i := 15; i >= 0; i-- {
		env.dbPool <- i
	}
	// Setup code here
	code := m.Run() // Run the tests
	// Teardown code here
	os.Exit(code)
}

type Env struct {
	dbPool chan int
	spec.Env[*redis_commands.RedisCommands]
}

func (e *Env) New(node string) (*redis_commands.RedisCommands, func()) {
	DB := <-e.dbPool
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       DB,
	})
	redisClient.FlushDB(context.Background())

	cmd := redis_commands.NewRedisCommands(redisClient)

	err := cmd.Set_current_node_key(context.Background(), node)

	if err != nil {
		panic(err)
	}

	return cmd, func() {
		redisClient.FlushDB(context.Background())
		redisClient.Close()
		e.dbPool <- DB
	}
}
