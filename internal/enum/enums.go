package enum

//go:generate go tool shoot enum -sql -json -type=Status

type Status int32

const StatusInvalid = -2

const (
	StatusFailed  Status = -1
	StatusPending Status = iota //1; not use 0
	StatusProcessing
	StatusSucceeded
)

//go:generate go tool shoot enum -json -type=PluginKind

type PluginKind int32

const (
	PluginKindSelf PluginKind = iota
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
)
