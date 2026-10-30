package xmysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
)

//go:generate go tool shoot map -path=../../internal/entity -to=Envelope,Envelope -type=Published,Received

type Published struct {
	ID            uint                // Unique envelope ID.
	CreatedAt     time.Time           // Creation time.
	Version       string              // Message version.
	Topic         string              // Routing topic.
	Status        enum.Status         // Message status.
	Headers       string              // Message metadata.
	Payload       string              // Serialized payload.
	Retries       int                 // Attempt count.
	LeaseToken    string              // Current lease token.
	LeaseUntil    sql.Null[time.Time] // Lease expiry time.
	NextAttemptAt time.Time           // Next eligible attempt time.
	ExpiredAt     sql.Null[time.Time]
	err           error
}

func (p *Published) writeEntity(e *entity.Envelope) {
	p.err = nil
	if p.LeaseUntil.Valid {
		e.LeaseUntil = p.LeaseUntil.V
	}
	e.NextAttemptAt = p.NextAttemptAt
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
	p.LeaseUntil = sql.Null[time.Time]{V: e.LeaseUntil, Valid: !e.LeaseUntil.IsZero()}
	p.NextAttemptAt = e.NextAttemptAt
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
	ID            uint                // Unique envelope ID.
	CreatedAt     time.Time           // Creation time.
	Version       string              // Message version.
	Topic         string              // Routing topic.
	Status        enum.Status         // Message status.
	Headers       string              // Message metadata.
	Payload       string              // Serialized payload.
	Retries       int                 // Retry count.
	LeaseToken    string              // Current lease token.
	LeaseUntil    sql.Null[time.Time] // Lease expiry time.
	NextAttemptAt time.Time           // Next eligible attempt time.
	ExpiredAt     sql.Null[time.Time]
	Group         string // Consumer group.
	err           error
}

func (r *Received) writeEntity(e *entity.Envelope) {
	r.err = nil
	if r.LeaseUntil.Valid {
		e.LeaseUntil = r.LeaseUntil.V
	}
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
	r.LeaseUntil = sql.Null[time.Time]{V: e.LeaseUntil, Valid: !e.LeaseUntil.IsZero()}
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
