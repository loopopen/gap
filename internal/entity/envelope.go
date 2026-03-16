package entity

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/errx"
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
	ID        uint
	CreatedAt time.Time
	Headers   map[string]string
	Version   string
	Topic     string
	Group     string
	Status    enum.Status
	Message   any
	Payload   []byte
	Retries   int
	Tag       any
}

func NewEnvelope(version string, topic string, msg any) *Envelope {
	id := sfNode.Generate().Int64()
	return &Envelope{
		ID:      uint(id),
		Version: version,
		Topic:   topic,
		Message: msg,
	}
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
