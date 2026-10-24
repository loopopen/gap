// Package enum exposes GAP enums to plugin implementations.
package enum

import internalenum "github.com/loopopen/gap/v2/internal/enum"

type Status = internalenum.Status
type PluginKind = internalenum.PluginKind
type Plugin = internalenum.Plugin

const (
	StatusInvalid    = internalenum.StatusInvalid
	StatusFailed     = internalenum.StatusFailed
	StatusPending    = internalenum.StatusPending
	StatusProcessing = internalenum.StatusProcessing
	StatusSucceeded  = internalenum.StatusSucceeded

	PluginKindSelf    = internalenum.PluginKindSelf
	PluginKindStorage = internalenum.PluginKindStorage
	PluginKindBroker  = internalenum.PluginKindBroker

	PluginNone     = internalenum.PluginNone
	PluginMySQL    = internalenum.PluginMySQL
	PluginGORM     = internalenum.PluginGORM
	PluginRabbitMQ = internalenum.PluginRabbitMQ
	PluginKafka    = internalenum.PluginKafka
	PluginPostgres = internalenum.PluginPostgres
)
