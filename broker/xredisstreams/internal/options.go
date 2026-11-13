package internal

import (
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/enum"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

//go:generate go tool shoot new -opt -short -type=Options

var _ ingap.BrokerPlugin = (*Options)(nil)

// Options configures a Redis Streams broker.
//
// A GAP topic is the Redis stream key, and the subscription group is the Redis
// consumer group gap.{version}.g.{group}. Each stream entry stores a JSON
// object in "headers" and the raw payload in "body". The group is created at
// "$" unless ReadFromBeginning is set, so a new group reads messages appended
// after it exists.
type Options struct {
	//@def=""
	Password string `yaml:"password"`

	//@def=""
	UserName string `yaml:"username"`

	// URL is a redis:// or rediss:// connection string. When it is set, Addrs,
	// UserName, Password, DB, and TLSConfig are ignored.
	URL string `yaml:"url"`

	//@def=[]string{"localhost:6379"}
	Addrs []string `yaml:"addrs"`

	//@def=0
	DB int `yaml:"db"`

	//@def=10
	PoolSize int `yaml:"pool_size"`

	// StreamEntriesCount is the XREADGROUP COUNT and the maximum number of
	// unsettled deliveries.
	//@def=10
	StreamEntriesCount int64 `yaml:"stream_entries_count"`

	// BlockTimeout is how long XREADGROUP waits for a new entry before the
	// consumer checks idle pending messages again.
	//@def=5*time.Second
	BlockTimeout time.Duration `yaml:"block_timeout"`

	// ReadFromBeginning creates the consumer group at stream id 0. The default
	// creates it at "$", so only entries appended after the group exists are read.
	ReadFromBeginning bool `yaml:"read_from_beginning"`

	// MaxLen trims the stream approximately to this many entries on publish.
	// Zero keeps every entry.
	//@def=0
	MaxLen int64 `yaml:"max_len"`

	//@def=nil
	TLSConfig *tls.Config `yaml:"-"`
}

func (o *Options) PluginType() enum.Plugin {
	return enum.PluginRedisStreams
}

func UseRedisStreams(opts ...shoot.Option[Options, *Options]) shoot.Option[ingap.BrokerOptions, *ingap.BrokerOptions] {
	return func(brokerOpts *ingap.BrokerOptions) {
		brokerOpts.Plugin = new(Options).With(opts...)
	}
}

func (o *Options) OpenProducer(gapOpts *ingap.Options) (broker.Producer, error) {
	producer, err := NewProducer(gapOpts, o)
	if err != nil {
		return nil, err
	}
	if err = producer.init(); err != nil {
		slog.Error("redisstreams: failed to init producer", slog.String("endpoint", o.logEndpoint()), slog.Any("err", err))
		return nil, err
	}
	return producer, nil
}

func (o *Options) OpenConsumer(gapOpts *ingap.Options, group string) (broker.Consumer, error) {
	consumer, err := NewConsumer(gapOpts, o, group)
	if err != nil {
		return nil, err
	}
	if err = consumer.init(); err != nil {
		slog.Error("redisstreams: failed to init consumer", slog.String("endpoint", o.logEndpoint()), slog.Any("err", err))
		return nil, err
	}
	return consumer, nil
}
