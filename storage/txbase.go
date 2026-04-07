package storage

import (
	"log/slog"
	"sync"

	"github.com/lopolopen/gap/internal/entity"
)

type TxerBase struct {
	mu           sync.Mutex
	envelopes    []*entity.Envelope
	flushHandler func(*entity.Envelope)
}

func (tx *TxerBase) SetFlushHandler(handler func(*entity.Envelope)) {
	tx.flushHandler = handler
}

func (tx *TxerBase) Flush() {
	if tx.flushHandler == nil {
		slog.Error("flush handler is not set")
		return
	}

	defer func() {
		if r := recover(); r != nil {
			slog.Error("recovered from panic when flushing", slog.Any("err", r))
		}
	}()

	slog.Debug("flush each envelope", slog.Int("count", len(tx.envelopes)))
	for _, e := range tx.envelopes {
		tx.flushHandler(e)
	}
}

func (s *TxerBase) Append(envelope *entity.Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.envelopes = append(s.envelopes, envelope)
}
