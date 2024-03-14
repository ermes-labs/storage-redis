package redis_commands

import (
	"context"

	"github.com/ermes-labs/api-go/api"
	"github.com/ermes-labs/api-go/infrastructure"
)

// Load the infrastructure.
func (rc *RedisCommands) LoadInfrastructure(
	ctx context.Context,
	infrastructure infrastructure.Infrastructure,
) (err error) {
	return nil
}

// Get the lookup node for a session offloading.
func (rc *RedisCommands) FindLookupNode(
	ctx context.Context,
	sessionId string,
) (nodeId string, err error) {
	return "", nil
}

// Get the resources usage of a session.
// errors:
// - ErrSessionNotFound: If no session with the given id is found.
func (rc *RedisCommands) GetSessionResourcesUsage(
	ctx context.Context,
	sessionId string,
) (resourcesUsage api.ResourcesUsage, err error) {
	return nil, nil
}

// Get the resources usage of a node.
func (rc *RedisCommands) GetNodeResourcesUsage(
	ctx context.Context,
	nodeId string,
) (sessions uint, resourcesUsage api.ResourcesUsage, err error) {
	return 0, nil, nil
}

// Update the resources usage of a session, this will also update the resources
// usage of the node.
// errors:
// - ErrSessionNotFound: If no session with the given id is found.
func (rc *RedisCommands) UpdateSessionResourcesUsage(
	ctx context.Context,
	sessionId string,
	resourcesUsage api.ResourcesUsage,
) (err error) {
	return nil
}

// Get the update to send to the parent node.
func (rc *RedisCommands) ResourcesUsageUpdateToParent(
	ctx context.Context,
) (host string, sessions uint, resourcesUsageNodesMap map[string]api.ResourcesUsage, err error) {
	return "", 0, nil, nil
}

// Get the update from the child nodes.
func (rc *RedisCommands) ResourcesUsageUpdateFromChild(
	ctx context.Context,
	sessions uint,
	resourcesUsageNodesMap map[string]api.ResourcesUsage,
) (err error) {
	return nil
}

// Redirect new requests to the best offload target.
func (rc *RedisCommands) RedirectNewRequests(
	ctx context.Context,
) (redirect bool, host string) {
	return false, ""
}
