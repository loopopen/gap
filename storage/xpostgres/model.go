package xpostgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/loopopen/gap/v2/spi/entity"
	"github.com/loopopen/gap/v2/spi/enum"
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
	err       error
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
	payload, err := e.PayloadBytes()
	if err != nil {
		p.err = fmt.Errorf("encode payload for id %d: %w", e.ID, err)
		return
	}
	p.Payload = string(payload)
	if p.Status == 0 {
		p.Status = enum.StatusPending
	}
	headers, err := e.HeadersBytes()
	if err != nil {
		p.err = fmt.Errorf("encode headers for id %d: %w", e.ID, err)
		return
	}
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
	err       error
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
	payload, err := e.PayloadBytes()
	if err != nil {
		r.err = fmt.Errorf("encode payload for id %d: %w", e.ID, err)
		return
	}
	r.Payload = string(payload)
	if r.Status == 0 {
		r.Status = enum.StatusPending
	}
	headers, err := e.HeadersBytes()
	if err != nil {
		r.err = fmt.Errorf("encode headers for id %d: %w", e.ID, err)
		return
	}
	r.Headers = string(headers)
}

func pubsToEntities(pubs []*Published) ([]*entity.Envelope, error) {
	es := make([]*entity.Envelope, 0, len(pubs))
	for _, p := range pubs {
		en := p.ToEntity()
		if p.err != nil {
			return nil, p.err
		}
		es = append(es, en)
	}
	return es, nil
}

func recsToEntities(recs []*Received) ([]*entity.Envelope, error) {
	es := make([]*entity.Envelope, 0, len(recs))
	for _, r := range recs {
		en := r.ToEntity()
		if r.err != nil {
			return nil, r.err
		}
		es = append(es, en)
	}
	return es, nil
}
