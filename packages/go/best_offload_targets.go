package redis_commands

import (
	"context"

	"github.com/ermes-labs/api-go/api"
	"github.com/ermes-labs/api-go/infrastructure"
)

// Return the best sessions to offload. This list is composed by the session
// chosen given the local context of the node (direct or indirect knowledge of
// the status of the system).
func (c *RedisCommands) BestSessionsToOffload(
	ctx context.Context,
	opt api.BestOffloadTargetsOptions,
) (sessions map[string]api.SessionInfoForOffloadDecision, err error) {
	return nil, nil
}

// Return the best offload targets composed by the session id and the node id.
// The options defines how the sessions are selected. Note that sessions and
// nodes may appear multiple times in the result, to allow for multiple choices
// of offload targets. those are not grouped by session id or node id to allow
// to express the priority of the offload targets.
func (c *RedisCommands) BestOffloadTargetNodes(
	ctx context.Context,
	nodeId string,
	sessions map[string]api.SessionInfoForOffloadDecision,
	opt api.BestOffloadTargetsOptions,
) ([][2]string, error) {
	return nil, nil
}

// Get the lookup node for a session offloading.
func (c *RedisCommands) FindLookupNode(
	ctx context.Context,
	sessionIds []string,
) (infrastructure.Node, error) {
	// FIXME: Implement this function correctly.
	nodeJson, err := c.client.FCall(ctx, "find_lookup_node", []string{sessionIds[0]}).Text()

	if err != nil {
		return infrastructure.Node{}, err
	}

	if nodeJson == "" {
		return infrastructure.Node{}, nil
	}

	node, err := infrastructure.UnmarshalNode([]byte(nodeJson))

	if err != nil {
		return infrastructure.Node{}, err
	}

	return *node, nil

}
