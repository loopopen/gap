package storage

import (
	"log/slog"
	"sync"

	"github.com/loopopen/gap/internal/entity"
)

// TxerBase collects envelopes published inside a transaction and flushes them after commit.
type TxerBase struct {
	mu           sync.Mutex
	envelopes    []*entity.Envelope
	flushHandler func(*entity.Envelope)
}

// SetFlushHandler sets the function called for each collected envelope.
func (tx *TxerBase) SetFlushHandler(handler func(*entity.Envelope)) {
	tx.flushHandler = handler
}

// Flush calls the handler for each appended envelope.
// A nil handler returns immediately. A panic from the handler is logged and stops the remaining envelopes.
func (tx *TxerBase) Flush() {
	if tx.flushHandler == nil {
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

// Append records an envelope to be flushed after the transaction commits.
func (s *TxerBase) Append(envelope *entity.Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.envelopes = append(s.envelopes, envelope)
}
