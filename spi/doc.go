// Package spi exposes the stable contracts used by independently versioned
// GAP broker and storage implementations. Application code should normally use
// package gap instead.
package spi

import gapinternal "github.com/loopopen/gap/v2/internal"

const (
	KeysMessageID     = gapinternal.KeysMessageID
	KeysMessageType   = gapinternal.KeysMessageType
	KeysGroup         = gapinternal.KeysGroup
	KeysCorrelationID = gapinternal.KeysCorrelationID
	KeysBrokerName    = gapinternal.KeysBrokerName
	KeysBrokerType    = gapinternal.KeysBrokerType
)
