package enum

//go:generate go tool shoot enum -sql -json -type=Status

type Status int32

const StatusInvalid = -2

const (
	StatusFailed  Status = -1
	StatusPending Status = iota //1
	StatusProcessing
	StatusSucceeded
)

//go:generate go tool shoot enum -json -type=MetaType

type MetaType int32

const (
	Self MetaType = iota
	Storage
	Broker
)

//go:generate go tool shoot enum -json -type=PluginType

type PluginType int32

const (
	None PluginType = iota
	MySQL
	GORM
	RabbitMQ
	Kafka
)
