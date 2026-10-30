package entity

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/errx"
	"github.com/loopopen/gap/internal/utility"
)

const (
	BrokerNameHeader = "gap-broker-name"
	BrokerTypeHeader = "gap-broker-type"
)

var sfNode *snowflake.Node
var initOnce sync.Once

func MustInitSnowflake(node int64) {
	initOnce.Do(func() {
		slog.Debug(fmt.Sprintf("snowflake picks node %d", node))
		var err error
		sfNode, err = snowflake.NewNode(node)
		if err != nil {
			panic(err)
		}
	})
}

type Envelope struct {
	ID            uint              // Unique envelope ID.
	CreatedAt     time.Time         // Creation time.
	Headers       map[string]string // Message metadata.
	Version       string            // Message version.
	Topic         string            // Routing topic.
	Group         string            // Consumer group.
	Status        enum.Status       // Message status.
	Message       any               // In-memory message.
	Payload       []byte            // Serialized payload.
	Retries       int               // Retry count.
	LeaseToken    string            // Current lease token.
	LeaseUntil    time.Time         // Lease expiry time.
	NextAttemptAt time.Time         // Next eligible attempt time.
	Tag           any               // Broker delivery metadata.
	Logger        *slog.Logger      // Envelope-scoped logger.
}

func NewEnvelope(version string, topic string, msg any) *Envelope {
	id := sfNode.Generate().Int64()
	return &Envelope{
		ID:      uint(id),
		Version: version,
		Topic:   topic,
		Message: msg,
		Logger:  slog.With("id", id),
	}
}

// WithLease initializes a new lease for the envelope.
func (e *Envelope) WithLease(ttl time.Duration) *Envelope {
	now := time.Now()
	e.LeaseToken = utility.GenFastToken(16)
	e.Status = enum.StatusPending
	e.LeaseUntil = now.Add(ttl)
	e.NextAttemptAt = now
	return e
}

func (e *Envelope) WithGroup(group string) *Envelope {
	e.Group = group
	return e
}

func (e *Envelope) WithPayload(payload []byte) *Envelope {
	e.Payload = payload
	return e
}

func (e *Envelope) WithTag(tag any) *Envelope {
	e.Tag = tag
	return e
}

func (e *Envelope) AddHeader(key string, value string) {
	if e.Headers == nil {
		e.Headers = make(map[string]string)
	}
	e.Headers[key] = value
}

func (e *Envelope) PayloadBytes() ([]byte, error) {
	if len(e.Payload) != 0 {
		return e.Payload, nil
	}
	if e.Message == nil {
		return nil, nil
	}
	var err error
	e.Payload, err = json.Marshal(e.Message)
	if err != nil {
		return nil, err
	}
	return e.Payload, nil
}

func (e *Envelope) HeadersBytes() ([]byte, error) {
	if len(e.Headers) == 0 {
		return nil, nil
	}
	return json.Marshal(e.Headers)
}

func (e *Envelope) IDString() string {
	return strconv.FormatInt(int64(e.ID), 10)
}

func (e *Envelope) Verify() error {
	if e.Topic == "" {
		return errx.ErrEmptyTopic
	}
	return nil
}

func (e *Envelope) Log() *slog.Logger {
	if e == nil || e.Logger == nil {
		return slog.Default()
	}
	return e.Logger
}
