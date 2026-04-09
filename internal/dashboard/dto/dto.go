package dto

import (
	"fmt"
	"time"

	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/pkgs/timex"
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
	Mapper
	ID        string
	CreatedAt timex.DateTime
	Version   string
	Topic     string
	Group     string
	Status    enum.Status
	Headers   string
	Payload   string
	Retries   int
}

func (m *Message) readEntity(e *entity.Envelope) {
	header, _ := e.HeadersBytes()
	m.Headers = string(header)
	m.ID = fmt.Sprintf("%d", e.ID)
}

type Mapper struct{}

// func (Mapper) DateTimeToTime(dt timex.DateTime) time.Time {
// 	return dt.Time
// }

func (Mapper) TimeToDateTime(t time.Time) timex.DateTime {
	return timex.DateTime{Time: t}
}

// func (Mapper) UIntToString(ui uint) string {
// 	return fmt.Sprintf("%d", ui)
// }
