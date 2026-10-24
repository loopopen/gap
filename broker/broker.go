package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/loopopen/gap/v2/internal/entity"
)

type Producer interface {
	Produce(ctx context.Context, envelope *entity.Envelope) error
}

// ConfigIdentifier can be implemented by broker options to distinguish live
// connection configurations. It must not be used as a persisted route name.
type ConfigIdentifier interface {
	ConfigIdentity() string
}

type Consumer interface {
	Subscribe(ctx context.Context, topic string) error

	Consume(ctx context.Context) (<-chan *entity.Envelope, error)

	Commit(tag any) error

	Reject(tag any) error
}

// ConfigIdentity returns a stable, non-sensitive identity for a broker
// configuration.
func ConfigIdentity(namespace, key string) string {
	sum := sha256.Sum256([]byte(key))
	return namespace + ":" + hex.EncodeToString(sum[:])
}
