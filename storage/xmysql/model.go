package xmysql

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
)

//go:generate go tool shoot map -path=../../internal/entity -to=Envelope,Envelope -type=Published,Received

type Published struct {
	ID        uint
	CreatedAt time.Time
	Version   string
	Topic     string
	Status    enum.Status
	Headers   string
	Payload   string
	Retries   int
	ExpiredAt sql.Null[time.Time]
}

func (p *Published) writeEntity(e *entity.Envelope) {
	if p.Headers != "" {
		var headers map[string]string
		err := json.Unmarshal([]byte(p.Headers), &headers)
		if err != nil {
			panic(err)
		}
		e.Headers = headers
	}
	e.Message = nil
	e.Group = ""
	e.Tag = nil
}

func (p *Published) readEntity(e *entity.Envelope) {
	if p.CreatedAt.IsZero() {
		p.CreatedAt = time.Now()
	}
	payload, _ := e.PayloadBytes()
	p.Payload = string(payload)
	if p.Status == 0 {
		p.Status = enum.StatusPending
	}
	headers, _ := e.HeadersBytes()
	p.Headers = string(headers)
}

type Received struct {
	ID        uint
	CreatedAt time.Time
	Version   string
	Topic     string
	Status    enum.Status
	Headers   string
	Payload   string
	Retries   int
	ExpiredAt sql.Null[time.Time]
	Group     string
}

func (r *Received) writeEntity(e *entity.Envelope) {
	if r.Headers != "" {
		var headers map[string]string
		err := json.Unmarshal([]byte(r.Headers), &headers)
		if err != nil {
			panic(err)
		}
		e.Headers = headers
	}
	e.Message = nil
	e.Tag = nil
}

func (r *Received) readEntity(e *entity.Envelope) {
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	payload, _ := e.PayloadBytes()
	r.Payload = string(payload)
	if r.Status == 0 {
		r.Status = enum.StatusPending
	}
	headers, _ := e.HeadersBytes()
	r.Headers = string(headers)
}
