// Package errx exposes errors used by GAP plugin implementations.
package errx

import internalerrx "github.com/loopopen/gap/v2/internal/errx"

var (
	ErrParamIsNil      = internalerrx.ErrParamIsNil
	ErrNoStorage       = internalerrx.ErrNoStorage
	ErrNoBroker        = internalerrx.ErrNoBroker
	ErrNilPayload      = internalerrx.ErrNilPayload
	ErrEmptyTopic      = internalerrx.ErrEmptyTopic
	ErrInvalidGormTx   = internalerrx.ErrInvalidGormTx
	ErrInvalidSQLTx    = internalerrx.ErrInvalidSQLTx
	ErrNilHandler      = internalerrx.ErrNilHandler
	ErrTxMultiBinding  = internalerrx.ErrTxMultiBinding
	ErrMultiHandlers   = internalerrx.ErrMultiHandlers
	ErrHandlerNotFound = internalerrx.ErrHandlerNotFound
)
