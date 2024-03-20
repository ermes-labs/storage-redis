package redis_commands

import (
	"bytes"
	"context"
	"io"

	"github.com/ermes-labs/api-go/api"
)

type OffloadData struct {
	String map[string]string            `json:"string,omitempty"`
	List   map[string][]string          `json:"list,omitempty"`
	Set    map[string][]string          `json:"set,omitempty"`
	ZSet   map[string][]string          `json:"zset,omitempty"`
	Hash   map[string]map[string]string `json:"hash,omitempty"`
}

// OffloadStart starts the offload of a session. The function returns the
// io.Reader that allows to read the session data, an optional loader function
// to fulfill the io.Reader, and an error. The function is thought to be
// used in scenarios where the session data is huge and streaming is
// required. The loader function will be run concurrently to the reader process.
// Errors can flow from the loader function to the reader passing trough the
// io.Reader, vice-versa the loader should stop if the context is canceled.
// errors:
// - ErrSessionNotFound: If no session with the given id is found.
// - ErrSessionIsOffloading: If the session is already offloading.
// - ErrUnableToOffloadAcquiredSession: If the session is unable to offload because it is acquired.
func (c *RedisCommands) OffloadSession(
	ctx context.Context,
	id string,
	opt api.OffloadSessionOptions,
) (io.ReadCloser, func(), error) {
	err := c.client.FCall(ctx, "offload_start", []string{id}).Err()

	if err != nil {
		return nil, nil, err
	}

	// TODO: implement cursor and use the loader function for each iteration after the first one.
	cursor := ""
	result, err := c.client.FCall(ctx, "offload_data", []string{id}, cursor).StringSlice()

	if err != nil {
		return nil, nil, err
	}

	offload_data := []byte(result[1])
	return io.NopCloser(bytes.NewReader(offload_data)), nil, nil
}

// Confirms the offload of a session.
// errors:
// - ErrSessionNotFound: If no session with the given id is found.
func (c *RedisCommands) ConfirmSessionOffload(
	ctx context.Context,
	id string,
	newLocation api.SessionLocation,
	opt api.OffloadSessionOptions,
	// TODO: extract into another API?
	notifyLastVisitedNode func(context.Context, api.SessionLocation) (bool, error),
) (err error) {
	return c.client.FCall(ctx, "offload_finish", []string{id}, newLocation.Host, newLocation.SessionId).Err()
}

// Updates the location of an offloaded session, the function returns true if
// the client has already been redirected to the new location, while the update
// is in progress. If true, this node is no more the last visited one, otherwise
// the node is still the last visited one.
// errors:
// - ErrSessionNotFound: If no session with the given id is found.
// - ErrSessionIsNotOffloaded: If the session is not offloaded.
func (c *RedisCommands) UpdateOffloadedSessionLocation(
	ctx context.Context,
	id string,
	newLocation api.SessionLocation,
) (bool, error) {
	return false, nil
}

// Returns the offloaded sessions, the function returns the new cursor, the
// list of session ids and an error. The cursor is used to paginate the results.
// If the cursor is empty, the function returns the first page of results.
// errors:
// - ErrInvalidCursor: If the cursor is invalid.
// - ErrInvalidCount: If the count is invalid.
func (c *RedisCommands) ScanOffloadedSessions(
	ctx context.Context,
	cursor uint64,
	count int64,
) (ids []string, newCursor uint64, err error) {
	results, newCursor, err := c.client.ZScan(ctx, "c:offloaded_sessions_set", cursor, "*", count).Result()
	if err != nil {
		return nil, 0, err
	}

	keys := make([]string, 0, len(results)/2)
	for i := 0; i < len(results); i += 2 {
		keys = append(keys, results[i])
	}

	return keys, newCursor, nil
}
