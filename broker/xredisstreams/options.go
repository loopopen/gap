package xredisstreams

import (
	"github.com/loopopen/gap/broker/xredisstreams/internal"
	"github.com/loopopen/gap/internal/dashboard"
	"github.com/loopopen/gap/internal/enum"
)

const version = "v1.0.0-beta.1"

type Options = internal.Options

var (
	UseRedisStreams    = internal.UseRedisStreams
	Password           = internal.Password
	UserName           = internal.UserName
	URL                = internal.URL
	Addrs              = internal.Addrs
	DB                 = internal.DB
	PoolSize           = internal.PoolSize
	StreamEntriesCount = internal.StreamEntriesCount
	BlockTimeout       = internal.BlockTimeout
	ReadFromBeginning  = internal.ReadFromBeginning
	MaxLen             = internal.MaxLen
	TLSConfig          = internal.TLSConfig
)

func init() {
	dashboard.AddMeta(enum.PluginKindBroker, enum.PluginRedisStreams, version)
}
