package redis_commands

import (
	"context"

	"github.com/ermes-labs/api-go/api"
)

// Returns the metadata associated with a session.
// errors:
// - ErrSessionNotFound: If no session with the given id is found.
func (c *RedisCommands) GetSessionMetadata(
	ctx context.Context,
	sessionId string,
) (api.SessionMetadata, error) {
	return api.SessionMetadata{}, nil
}

// SetClientCoordinates sets the coordinates of the client of a session.
// errors:
// - ErrSessionNotFound: If no session with the given id is found.
func (c *RedisCommands) SetSessionMetadata(
	ctx context.Context,
	sessionId string,
	opt api.SessionMetadataOptions,
) error {
	return nil
}
