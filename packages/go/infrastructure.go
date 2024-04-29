package redis_commands

import (
	"context"

	"github.com/ermes-labs/api-go/api"
	"github.com/ermes-labs/api-go/infrastructure"
)

// Load the infrastructure.
func (c *RedisCommands) LoadInfrastructure(
	ctx context.Context,
	infra infrastructure.Infrastructure,
) (err error) {
	areas := infra.Flatten()

	for _, area := range areas {
		// marshall node to json
		nodeJson, err := infrastructure.MarshalNode(area.Node)

		if err != nil {
			return err
		}

		if err := c.client.FCall(ctx, "register_node", []string{area.AreaName}, string(nodeJson)).Err(); err != nil {
			return err
		}

		if area.Areas != nil {
			for _, subArea := range area.Areas {
				if err := c.client.FCall(ctx, "register_node_relation", []string{area.AreaName, subArea.AreaName}).Err(); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Get the parent node of a node.
func (c *RedisCommands) GetParentNodeOf(
	ctx context.Context,
	nodeId string,
) (*infrastructure.Node, error) {
	parentJson, err := c.client.FCall(ctx, "get_parent_node_of", []string{nodeId}).Text()

	if err != nil {
		return &infrastructure.Node{}, err
	}

	if parentJson == "" {
		return nil, nil
	}

	node, err := infrastructure.UnmarshalNode([]byte(parentJson))

	if err != nil {
		return &infrastructure.Node{}, err
	}

	return node, nil
}

// Get the children nodes of a node.
func (c *RedisCommands) GetChildrenNodesOf(
	ctx context.Context,
	nodeId string,
) ([]infrastructure.Node, error) {
	childrenJson, err := c.client.FCall(ctx, "get_children_nodes_of", []string{nodeId}).StringSlice()

	if err != nil {
		return nil, err
	}

	var nodes []infrastructure.Node

	for _, childJson := range childrenJson {
		node, err := infrastructure.UnmarshalNode([]byte(childJson))

		if err != nil {
			return nil, err
		}

		nodes = append(nodes, *node)
	}

	return nodes, nil
}

// Get the resources usage of a session.
// errors:
// - ErrSessionNotFound: If no session with the given id is found.
func (c *RedisCommands) GetSessionResourcesUsage(
	ctx context.Context,
	sessionId string,
) (resourcesUsage api.ResourcesUsage, err error) {
	return nil, nil
}

// Get the resources usage of a node.
func (c *RedisCommands) GetNodeResourcesUsage(
	ctx context.Context,
	nodeId string,
) (sessions uint, resourcesUsage api.ResourcesUsage, err error) {
	return 0, nil, nil
}

// Update the resources usage of a session, this will also update the resources
// usage of the node.
// errors:
// - ErrSessionNotFound: If no session with the given id is found.
func (c *RedisCommands) UpdateSessionResourcesUsage(
	ctx context.Context,
	sessionId string,
	resourcesUsage api.ResourcesUsage,
) (err error) {
	return nil
}

// Get the update to send to the parent node.
func (c *RedisCommands) ResourcesUsageUpdateToParent(
	ctx context.Context,
) (node infrastructure.Node, sessions uint, resourcesUsageNodesMap map[string]api.ResourcesUsage, err error) {
	return infrastructure.Node{}, 0, nil, nil
}

// Get the update from the child nodes.
func (c *RedisCommands) ResourcesUsageUpdateFromChild(
	ctx context.Context,
	sessions uint,
	resourcesUsageNodesMap map[string]api.ResourcesUsage,
) (err error) {
	return nil
}
