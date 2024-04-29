package redis_commands

import "fmt"

// Struct that contains all the public key spaces of Ermes.
type PublicErmesKeySpaces struct {
	Session keySpace
	Node    keySpace
}

// Struct that contains all the internal key spaces of Ermes.
type InternalErmesKeySpaces struct {
	SessionMetadata keySpace
	Config          keySpace
}

// Struct that contains all the key spaces of Ermes.
type ErmesKeySpaces struct {
	PublicErmesKeySpaces
	InternalErmesKeySpaces
}

// Create a PublicErmesKeySpaces struct.
func NewPublicErmesKeySpaces(sessionId string) PublicErmesKeySpaces {
	if len(sessionId) != 36 {
		panic("session id must be 36 characters long")
	}

	return PublicErmesKeySpaces{
		Session: NewKeySpace("s:" + sessionId + ":"),
		Node:    NewKeySpace("n:"),
	}
}

// Create a PublicErmesKeySpaces struct without session specific key spaces.
func NewPublicErmesKeySpaceWithoutSessionSpecificKeySpaces() PublicErmesKeySpaces {
	return PublicErmesKeySpaces{
		Session: panicUnsetSessionIdError,
		Node:    NewKeySpace("n:"),
	}
}

// Create a InternalErmesKeySpaces struct.
func NewInternalErmesKeySpaces(sessionId string) InternalErmesKeySpaces {
	if len(sessionId) != 36 {
		panic("session id must be 36 characters long")
	}

	return InternalErmesKeySpaces{
		SessionMetadata: NewKeySpace("m:" + sessionId + ":"),
		Config:          NewKeySpace("c:"),
	}
}

// Create a InternalErmesKeySpaces struct without session specific key spaces.
func NewInternalErmesKeySpaceWithoutSessionSpecificKeySpaces() InternalErmesKeySpaces {
	return InternalErmesKeySpaces{
		SessionMetadata: panicUnsetSessionIdError,
		Config:          NewKeySpace("c:"),
	}
}

// Create a ErmesKeySpaces struct.
func NewErmesKeySpaces(sessionId string) ErmesKeySpaces {
	if len(sessionId) != 36 {
		panic("session id must be 36 characters long")
	}

	return ErmesKeySpaces{
		PublicErmesKeySpaces:   NewPublicErmesKeySpaces(sessionId),
		InternalErmesKeySpaces: NewInternalErmesKeySpaces(sessionId),
	}
}

// Create a ErmesKeySpaces struct without session specific key spaces.
func NewErmesKeySpacesWithoutSessionSpecificKeySpaces() ErmesKeySpaces {
	return ErmesKeySpaces{
		PublicErmesKeySpaces:   NewPublicErmesKeySpaceWithoutSessionSpecificKeySpaces(),
		InternalErmesKeySpaces: NewInternalErmesKeySpaceWithoutSessionSpecificKeySpaces(),
	}
}

// Panic with a message that the session id is not set.
func panicUnsetSessionIdError(prefix string) string {
	panic("session id is not set")
}

// Key mapper function.
type keySpace func(key string) string

// Create a new KeySpace from a prefix.
func NewKeySpace(prefix string) keySpace {
	return func(key string) string {
		return prefix + key
	}
}

// Check if a key is in the key space.
func (keySpace *keySpace) Is(key string) bool {
	prefix := (*keySpace)("")
	return prefix == key[:len(prefix)]
}

// Unwrap a key from the key space.
func (keySpace *keySpace) Unwrap(key string) (string, error) {
	prefix := (*keySpace)("")
	if !keySpace.Is(key) {
		return "", fmt.Errorf("key %s is not in the key space %s", key, prefix)
	}

	// Remove the prefix from the key and return the unwrapped key.
	return key[len(prefix):], nil
}
