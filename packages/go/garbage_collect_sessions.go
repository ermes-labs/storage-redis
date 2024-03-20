package redis_commands

import (
	"context"

	"github.com/ermes-labs/api-go/api"
)

// Garbage collect sessions, the options to define how the sessions are
// garbage collected. The function accept a cursor to continue the garbage
// collection from the last cursor, nil to start from the beginning. The
// function returns the next cursor to continue the garbage collection, or
// nil if the garbage collection is completed.
func (c *RedisCommands) GarbageCollectSessions(
	ctx context.Context,
	opt api.GarbageCollectSessionsOptions,
	cursor *string,
) (*string, error) {
	return nil, nil
}
