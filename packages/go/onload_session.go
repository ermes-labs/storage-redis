package redis_commands

import (
	"context"
	"io"

	"github.com/ermes-labs/api-go/api"
)

// StartOnload starts the onload of a session and returns the id of the
// session.
// errors:
// - ErrSessionNotFound: If no session with the given id is found.
// - ErrSessionAlreadyOnloaded: If the session is already onloaded.
func (c *RedisCommands) OnloadSession(
	ctx context.Context,
	metadata api.SessionMetadata,
	reader io.Reader,
	opt api.OnloadSessionOptions,
) (string, error) {
	return "", nil
}
