package model

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/utility"
)

type Model struct {
	ID            uint                `gorm:"primarykey"`
	CreatedAt     time.Time           `gorm:"not null"`
	Version       string              `gorm:"not null;size:16"`
	Topic         string              `gorm:"not null;size:256"`
	BrokerName    string              `gorm:"not null;size:128"`
	Status        Status              `gorm:"not null"`
	Headers       string              `gorm:"type:text"`
	Payload       Payload             ``
	Retries       int                 `gorm:"not null;default:0"`
	LeaseToken    string              `gorm:"size:32"`
	LeaseUntil    sql.Null[time.Time] ``
	NextAttemptAt time.Time           `gorm:"not null"`
	ExpiredAt     sql.Null[time.Time] ``
	err           error               `gorm:"-"`
}

func (m *Model) Err() error {
	return m.err
}

//go:generate go tool shoot map -path=../../../../internal/entity -to=Envelope,Envelope -type=Published,Received

type Published struct {
	utility.Mapper `gorm:"-"`
	Model
}

func (p *Published) writeEntity(e *entity.Envelope) {
	p.err = nil
	if p.Headers != "" {
		var headers map[string]string
		if err := json.Unmarshal([]byte(p.Headers), &headers); err != nil {
			p.err = fmt.Errorf("decode headers for id %d: %w", p.ID, err)
			return
		}
		e.Headers = headers
	}
	e.Message = nil
	e.Group = ""
	e.Tag = nil
	e.Logger = slog.With("id", e.ID)
}

func (p *Published) readEntity(e *entity.Envelope) {
	p.err = nil
	if p.CreatedAt.IsZero() {
		p.CreatedAt = time.Now()
	}
	serialized, err := e.PayloadBytes()
	if err != nil {
		p.err = fmt.Errorf("encode payload for id %d: %w", e.ID, err)
		return
	}
	p.Payload = Payload(serialized)
	if p.Status == 0 {
		p.Status = Status(enum.StatusPending)
	}
	headers, err := e.HeadersBytes()
	if err != nil {
		p.err = fmt.Errorf("encode headers for id %d: %w", e.ID, err)
		return
	}
	p.Headers = string(headers)
}

type Received struct {
	utility.Mapper `gorm:"-"`
	Model
	Group string `gorm:"not null;size:128"` // Consumer group.
}

func (r *Received) writeEntity(e *entity.Envelope) {
	r.err = nil
	if r.Headers != "" {
		var headers map[string]string
		if err := json.Unmarshal([]byte(r.Headers), &headers); err != nil {
			r.err = fmt.Errorf("decode headers for id %d: %w", r.ID, err)
			return
		}
		e.Headers = headers
	}
	e.Message = nil
	e.Tag = nil
	e.Logger = slog.With("id", e.ID)
}

func (r *Received) readEntity(e *entity.Envelope) {
	r.err = nil
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	serialized, err := e.PayloadBytes()
	if err != nil {
		r.err = fmt.Errorf("encode payload for id %d: %w", e.ID, err)
		return
	}
	r.Payload = Payload(serialized)
	if r.Status == 0 {
		r.Status = Status(enum.StatusPending)
	}
	headers, err := e.HeadersBytes()
	if err != nil {
		r.err = fmt.Errorf("encode headers for id %d: %w", e.ID, err)
		return
	}
	r.Headers = string(headers)
}

func PublishedToEntities(rows []*Published) ([]*entity.Envelope, error) {
	result := make([]*entity.Envelope, 0, len(rows))
	for _, row := range rows {
		envelope := row.ToEntity()
		if row.err != nil {
			return nil, row.err
		}
		result = append(result, envelope)
	}
	return result, nil
}

func ReceivedToEntities(rows []*Received) ([]*entity.Envelope, error) {
	result := make([]*entity.Envelope, 0, len(rows))
	for _, row := range rows {
		envelope := row.ToEntity()
		if row.err != nil {
			return nil, row.err
		}
		result = append(result, envelope)
	}
	return result, nil
}
