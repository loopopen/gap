package xgorm

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
)

type Status enum.Status

type Model struct {
	ID        uint        `gorm:"primarykey"`
	CreatedAt time.Time   `gorm:"not null"`
	Version   string      `gorm:"not null;size:16"`
	Topic     string      `gorm:"not null;size:256"`
	Status    enum.Status `gorm:"not null;type:ENUM('Pending','Processing','Succeeded','Failed')"`
	Headers   string      `gorm:"type:text"`
	Payload   string      `gorm:"type:longtext"`
	Retries   int         `gorm:"default:0"`
	ExpiredAt sql.Null[time.Time]
}

//go:generate go tool shoot map -path=../../internal/entity -to=Envelope,Envelope -type=Published,Received

type Published struct {
	Model
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
	e.Logger = slog.With("id", e.ID)
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
	Model
	Group string `gorm:"not null;size:128"`
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
	e.Logger = slog.With("id", e.ID)
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
