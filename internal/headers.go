package internal

import (
	"errors"

	"github.com/loopopen/gap/v2/internal/entity"
)

const (
	KeysMessageID     = "gap-msg-id"
	KeysMessageType   = "gap-msg-type"
	KeysGroup         = "gap-group"
	KeysCorrelationID = "gap-corr-id"
	KeysBrokerName    = entity.BrokerNameHeader
	KeysBrokerType    = entity.BrokerTypeHeader
)

var errInvalidArgs = errors.New("args must be one string map or several string pairs")

type Headers struct {
	headers map[string]string
}

func (h *Headers) Add(args ...any) {
	if len(args) == 0 {
		return
	}

	if h.headers == nil {
		h.headers = make(map[string]string)
	}

	if len(args) == 1 {
		if m, ok := args[0].(map[string]string); ok {
			for k, v := range m {
				h.headers[k] = v
			}
			return
		}
	}

	for _, arg := range args {
		p, ok := arg.(pair)
		if !ok {
			panic(errInvalidArgs)
		}
		if _, ok := h.headers[p.Key]; !ok {
			h.headers[p.Key] = p.Value
		}
	}
}

func (h *Headers) Value() map[string]string {
	return h.headers
}
