package dto

import (
	"fmt"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/pkgs/timex"
	"github.com/loopopen/gap/internal/utility"
)

//go:generate go tool shoot new -json -file=$GOFILE

type PagedResult[T any] struct {
	Data       []T
	Pagination *entity.Pagination
}

type Meta struct {
	Typ     enum.PluginKind `json:"type"`
	Plugin  enum.Plugin
	Version string
}

//go:generate go tool shoot map -path=../../entity -way=<- -to=Envelope -type=Message

type Message struct {
	utility.Mapper
	ID        string         // Unique envelope ID.
	CreatedAt timex.DateTime // Creation time.
	Version   string         // Message version.
	Topic     string         // Routing topic.
	Group     string         // Consumer group.
	Status    enum.Status    // Message status.
	Headers   string         // Message metadata.
	Payload   string         // Serialized payload.
	Retries   int            // Retry count.
}

func (m *Message) readEntity(e *entity.Envelope) {
	header, _ := e.HeadersBytes()
	m.Headers = string(header)
	m.ID = fmt.Sprintf("%d", e.ID)
}
