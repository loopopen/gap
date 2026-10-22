package internal

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func clientConfig(opts *Options) kafka.ConfigMap {
	config := make(kafka.ConfigMap, len(opts.Config)+5)
	for key, value := range opts.Config {
		config[key] = value
	}
	config["bootstrap.servers"] = strings.Join(opts.Brokers, ",")
	if opts.UserName != "" || opts.Password != "" {
		config["sasl.username"] = opts.UserName
		config["sasl.password"] = opts.Password
		if _, ok := config["sasl.mechanisms"]; !ok {
			config["sasl.mechanisms"] = "PLAIN"
		}
		if _, ok := config["security.protocol"]; !ok {
			config["security.protocol"] = "SASL_PLAINTEXT"
		}
	}
	return config
}

func consumerConfig(opts *Options, groupID string) (kafka.ConfigMap, error) {
	config := clientConfig(opts)
	config["group.id"] = groupID
	config["enable.auto.commit"] = false
	config["enable.auto.offset.store"] = false

	switch kafka.Offset(opts.StartOffset) {
	case kafka.OffsetBeginning:
		config["auto.offset.reset"] = "earliest"
	case kafka.OffsetEnd:
		config["auto.offset.reset"] = "latest"
	default:
		return nil, fmt.Errorf("confluent: unsupported start offset %d", opts.StartOffset)
	}
	return config, nil
}

func (o *Options) connectionKey() string {
	brokers := slices.Clone(o.Brokers)
	slices.Sort(brokers)
	parts := []string{
		strings.Join(brokers, ","),
		o.UserName,
		o.Password,
	}
	if len(o.Config) > 0 {
		keys := make([]string, 0, len(o.Config))
		for key := range o.Config {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			parts = append(parts, key+"="+fmt.Sprint(o.Config[key]))
		}
	}
	return strings.Join(parts, "\x00")
}

func (o *Options) producerKey() string {
	return o.connectionKey()
}

func (o *Options) consumerKey(version, group string) string {
	return strings.Join([]string{
		o.connectionKey(),
		version,
		group,
		strconv.FormatInt(int64(o.StartOffset), 10),
	}, "\x00")
}

var (
	adminMu  sync.Mutex
	adminMap = map[string]*kafka.AdminClient{}
)

func SingleAdmin(opts *Options) (*kafka.AdminClient, error) {
	key := opts.connectionKey()
	adminMu.Lock()
	if existing, ok := adminMap[key]; ok {
		adminMu.Unlock()
		return existing, nil
	}
	adminMu.Unlock()

	config := clientConfig(opts)
	created, err := kafka.NewAdminClient(&config)
	if err != nil {
		return nil, err
	}

	adminMu.Lock()
	defer adminMu.Unlock()
	if existing, ok := adminMap[key]; ok {
		created.Close()
		return existing, nil
	}
	adminMap[key] = created
	return created, nil
}
