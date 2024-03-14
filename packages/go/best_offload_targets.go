package redis_commands

import (
	"context"

	"github.com/ermes-labs/api-go/api"
)

// Return the best offload targets composed by the session id and the node id.
// The options defines how the sessions are selected. Note that sessions and
// nodes may appear multiple times in the result, to allow for multiple choices
// of offload targets. those are not grouped by session id or node id to allow
// to express the priority of the offload targets.
func (c *RedisCommands) BestOffloadTargetNodes(
	ctx context.Context,
	sessions map[string]api.SessionMetadata,
	opt api.BestOffloadTargetsOptions,
) ([][2]string, error) {
	return nil, nil
}

// Return the best sessions to offload. This list is composed by the session
// chosen given the local context of the node (direct or indirect knowledge of
// the status of the system).
func (c *RedisCommands) BestSessionsToOffload(
	ctx context.Context,
	opt api.BestOffloadTargetsOptions,
) (sessions map[string]api.SessionInfoForOffloadDecision, err error) {
	return nil, nil
}
