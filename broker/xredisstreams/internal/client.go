package internal

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/loopopen/gap/broker"
	"github.com/redis/go-redis/v9"
)

const (
	defaultAddr               = "localhost:6379"
	defaultPoolSize           = 10
	defaultStreamEntriesCount = 10
	defaultBlockTimeout       = 5 * time.Second

	streamStartNew       = "$"
	streamStartBeginning = "0"
	readNew              = ">"
	readPending          = "0"
	claimStart           = "0-0"
)

type streamEntry struct {
	Stream string
	ID     string
	Values map[string]any
}

// streamClient is the Redis Streams surface used by the producer and consumer:
// create the consumer group, XADD, XREADGROUP, XACK, and reclaim idle pending
// entries.
type streamClient interface {
	Ping(ctx context.Context) error
	EnsureGroup(ctx context.Context, stream, group, start string) error
	Add(ctx context.Context, stream string, maxLen int64, values map[string]any) (string, error)
	ReadGroup(ctx context.Context, group, consumer string, streams []string, id string, count int64, block time.Duration) ([]streamEntry, error)
	Ack(ctx context.Context, stream, group, id string) error
	AutoClaim(ctx context.Context, stream, group, consumer, start string, minIdle time.Duration, count int64) ([]streamEntry, string, error)
}

type redisClient struct {
	rdb redis.UniversalClient
}

var (
	clientMu  sync.Mutex
	clientMap = map[string]*redisClient{}
)

func (o *Options) poolSize() int {
	if o == nil || o.PoolSize < 1 {
		return defaultPoolSize
	}
	return o.PoolSize
}

func (o *Options) entriesCount() int64 {
	if o == nil || o.StreamEntriesCount < 1 {
		return defaultStreamEntriesCount
	}
	return o.StreamEntriesCount
}

func (o *Options) blockTimeout() time.Duration {
	if o == nil || o.BlockTimeout <= 0 {
		return defaultBlockTimeout
	}
	return o.BlockTimeout
}

func (o *Options) groupStart() string {
	if o != nil && o.ReadFromBeginning {
		return streamStartBeginning
	}
	return streamStartNew
}

func (o *Options) addrs() []string {
	if o == nil || len(o.Addrs) == 0 {
		return []string{defaultAddr}
	}
	return o.Addrs
}

func (o *Options) logEndpoint() string {
	if o == nil {
		return ""
	}
	if o.URL != "" {
		parsed, err := url.Parse(o.URL)
		if err != nil || parsed.Host == "" {
			return "invalid-url"
		}
		return parsed.Host
	}
	return strings.Join(o.addrs(), ",")
}

func (o *Options) connectionKey() string {
	pool := strconv.Itoa(o.poolSize())
	if o.URL != "" {
		return strings.Join([]string{"url", o.URL, pool}, "\x00")
	}
	addrs := slices.Clone(o.addrs())
	slices.Sort(addrs)
	tlsID := ""
	if o.TLSConfig != nil {
		tlsID = fmt.Sprintf("%p", o.TLSConfig)
	}
	return strings.Join([]string{
		strings.Join(addrs, ","),
		o.UserName,
		o.Password,
		strconv.Itoa(o.DB),
		pool,
		tlsID,
	}, "\x00")
}

func (o *Options) ConfigID() string {
	return broker.ConfigID("xredisstreams", o.connectionKey())
}

func (o *Options) universalOptions() (*redis.UniversalOptions, error) {
	pool := o.poolSize()
	if o.URL != "" {
		parsed, err := redis.ParseURL(o.URL)
		if err != nil {
			return nil, fmt.Errorf("redisstreams: parse url: %w", err)
		}
		return &redis.UniversalOptions{
			Addrs:        []string{parsed.Addr},
			Username:     parsed.Username,
			Password:     parsed.Password,
			DB:           parsed.DB,
			TLSConfig:    parsed.TLSConfig,
			PoolSize:     pool,
			MaxRetries:   -1, // disable client retries; XADD is not idempotent
			DialTimeout:  3 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		}, nil
	}

	var tlsConfig *tls.Config
	if o.TLSConfig != nil {
		tlsConfig = o.TLSConfig.Clone()
	}
	return &redis.UniversalOptions{
		Addrs:        append([]string(nil), o.addrs()...),
		Username:     o.UserName,
		Password:     o.Password,
		DB:           o.DB,
		TLSConfig:    tlsConfig,
		PoolSize:     pool,
		MaxRetries:   -1, // disable client retries; XADD is not idempotent
		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}, nil
}

func newRedisClient(opts *Options) (*redisClient, error) {
	universal, err := opts.universalOptions()
	if err != nil {
		return nil, err
	}
	return &redisClient{rdb: redis.NewUniversalClient(universal)}, nil
}

func SingleClient(opts *Options) (*redisClient, error) {
	key := opts.connectionKey()
	clientMu.Lock()
	defer clientMu.Unlock()
	if existing, ok := clientMap[key]; ok {
		return existing, nil
	}
	created, err := newRedisClient(opts)
	if err != nil {
		return nil, err
	}
	clientMap[key] = created
	return created, nil
}

func (c *redisClient) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func (c *redisClient) EnsureGroup(ctx context.Context, stream, group, start string) error {
	err := c.rdb.XGroupCreateMkStream(ctx, stream, group, start).Err()
	if err == nil || isBusyGroup(err) {
		return nil
	}
	return err
}

func (c *redisClient) Add(ctx context.Context, stream string, maxLen int64, values map[string]any) (string, error) {
	args := &redis.XAddArgs{
		Stream: stream,
		ID:     "*",
		Values: values,
	}
	if maxLen > 0 {
		args.MaxLen = maxLen
		args.Approx = true
	}
	return c.rdb.XAdd(ctx, args).Result()
}

func (c *redisClient) ReadGroup(ctx context.Context, group, consumer string, streams []string, id string, count int64, block time.Duration) ([]streamEntry, error) {
	if len(streams) == 0 {
		return nil, nil
	}
	args := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  streamReadArgs(streams, id),
		Count:    count,
		// Negative Block omits BLOCK, so the read returns immediately.
		// Zero would send BLOCK 0 and wait forever.
		Block: block,
	}
	result, err := c.rdb.XReadGroup(ctx, args).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	entries := make([]streamEntry, 0)
	for _, stream := range result {
		for _, message := range stream.Messages {
			entries = append(entries, streamEntry{
				Stream: stream.Stream,
				ID:     message.ID,
				Values: message.Values,
			})
		}
	}
	return entries, nil
}

func (c *redisClient) Ack(ctx context.Context, stream, group, id string) error {
	return c.rdb.XAck(ctx, stream, group, id).Err()
}

func (c *redisClient) AutoClaim(ctx context.Context, stream, group, consumer, start string, minIdle time.Duration, count int64) ([]streamEntry, string, error) {
	if start == "" {
		start = claimStart
	}
	messages, next, err := c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    start,
		Count:    count,
	}).Result()
	if err != nil {
		return nil, "", err
	}
	entries := make([]streamEntry, 0, len(messages))
	for _, message := range messages {
		entries = append(entries, streamEntry{
			Stream: stream,
			ID:     message.ID,
			Values: message.Values,
		})
	}
	return entries, next, nil
}

func streamReadArgs(streams []string, id string) []string {
	args := make([]string, 0, len(streams)*2)
	args = append(args, streams...)
	for range streams {
		args = append(args, id)
	}
	return args
}

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

func isNoGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}

func claimUnsupported(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToUpper(err.Error())
	return strings.Contains(message, "UNKNOWN COMMAND") || strings.Contains(message, "ERR UNKNOWN")
}
