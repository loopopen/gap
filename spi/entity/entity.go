// Package entity exposes message entities to GAP plugin implementations.
package entity

import internalentity "github.com/loopopen/gap/v2/internal/entity"

type Envelope = internalentity.Envelope
type MessageFilter = internalentity.MessageFilter
type MessageStats = internalentity.MessageStats
type DashboardStats = internalentity.DashboardStats
type Pagination = internalentity.Pagination

const (
	BrokerNameHeader = internalentity.BrokerNameHeader
	BrokerTypeHeader = internalentity.BrokerTypeHeader
	MaxPageSize      = internalentity.MaxPageSize
	DefPageSize      = internalentity.DefPageSize
)

var NewEnvelope = internalentity.NewEnvelope
var NewPagination = internalentity.NewPagination
var MustInitSnowflake = internalentity.MustInitSnowflake
