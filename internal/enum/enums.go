package enum

//go:generate go tool shoot enum -sql -json -type=Status

type Status int32

const StatusInvalid = -2

const (
	StatusFailed  Status = -1
	StatusPending Status = iota //1; not use 0
	StatusSucceeded
)

//go:generate go tool shoot enum -json -type=PluginKind

type PluginKind int32

const (
	PluginKindGap PluginKind = iota
	PluginKindStorage
	PluginKindBroker
)

//go:generate go tool shoot enum -json -type=Plugin

type Plugin int32

const (
	PluginNone Plugin = iota
	PluginMySQL
	PluginGORM
	PluginRabbitMQ
	PluginKafka
	PluginPostgres
)
