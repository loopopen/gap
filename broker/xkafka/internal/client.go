package internal

import (
	"crypto/tls"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/loopopen/gap/v2/broker"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type kafkaConnections struct {
	client    *kafka.Client
	dialer    *kafka.Dialer
	transport *kafka.Transport
}

var (
	connMu  sync.Mutex
	connMap = map[string]*kafkaConnections{}
)

func (o *Options) connectionKey() string {
	brokers := slices.Clone(o.Brokers)
	slices.Sort(brokers)
	tlsID := ""
	if o.TLSConfig != nil {
		tlsID = fmt.Sprintf("%p", o.TLSConfig)
	}
	return strings.Join([]string{
		strings.Join(brokers, ","),
		o.UserName,
		o.Password,
		tlsID,
	}, "\x00")
}

func (o *Options) producerKey() string {
	return o.connectionKey()
}

func (o *Options) ConfigIdentity() string {
	return broker.ConfigIdentity("xkafka", o.producerKey())
}

func (o *Options) consumerKey(version, group string) string {
	return strings.Join([]string{
		o.connectionKey(),
		version,
		group,
		strconv.FormatInt(o.StartOffset, 10),
	}, "\x00")
}

func newKafkaConnections(opts *Options) *kafkaConnections {
	var mechanism sasl.Mechanism
	if opts.UserName != "" && opts.Password != "" {
		mechanism = plain.Mechanism{
			Username: opts.UserName,
			Password: opts.Password,
		}
	}

	var tlsConfig *tls.Config
	if opts.TLSConfig != nil {
		tlsConfig = opts.TLSConfig.Clone()
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConfig,
		SASLMechanism: mechanism,
	}
	transport := &kafka.Transport{
		DialTimeout: 10 * time.Second,
		TLS:         tlsConfig,
		SASL:        mechanism,
	}

	return &kafkaConnections{
		client: &kafka.Client{
			Addr:      kafka.TCP(opts.Brokers...),
			Timeout:   10 * time.Second,
			Transport: transport,
		},
		dialer:    dialer,
		transport: transport,
	}
}

func singleKafkaConnections(opts *Options) *kafkaConnections {
	key := opts.connectionKey()
	connMu.Lock()
	defer connMu.Unlock()
	if existing, ok := connMap[key]; ok {
		return existing
	}
	created := newKafkaConnections(opts)
	connMap[key] = created
	return created
}

func SingleClient(opts *Options) *kafka.Client {
	return singleKafkaConnections(opts).client
}

func SingleDialer(opts *Options) *kafka.Dialer {
	return singleKafkaConnections(opts).dialer
}

func SingleTransport(opts *Options) *kafka.Transport {
	return singleKafkaConnections(opts).transport
}
