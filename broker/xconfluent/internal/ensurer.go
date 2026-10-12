package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	topicEnsureTimeout = 30 * time.Second
	topicCreateRetries = 5
	topicValidateDelay = 500 * time.Millisecond
	metadataTimeout    = 10 * time.Second
)

type adminClient interface {
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
	CreateTopics(context.Context, []kafka.TopicSpecification, ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error)
}

type Ensurer struct {
	opts        *Options
	topicOpts   *TopicOptions
	admin       adminClient
	topicsCache map[string]bool
	topicCalls  map[string]*topicCall
	cacheMu     sync.RWMutex
}

type topicCall struct {
	done chan struct{}
	err  error
}

func newEnsurer(opts *Options, admin adminClient) *Ensurer {
	return &Ensurer{
		opts:        opts,
		topicOpts:   opts.TopicOpts,
		admin:       admin,
		topicsCache: make(map[string]bool),
		topicCalls:  make(map[string]*topicCall),
	}
}

var (
	ensurerOnce sync.Once
	ensurer     *Ensurer
	ensurerErr  error
)

func SingleEnsurer(opts *Options) (*Ensurer, error) {
	ensurerOnce.Do(func() {
		var admin *kafka.AdminClient
		admin, ensurerErr = SingleAdmin(opts)
		if ensurerErr == nil {
			ensurer = newEnsurer(opts, admin)
		}
	})
	return ensurer, ensurerErr
}

func (e *Ensurer) ensure(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	meta, err := e.admin.GetMetadata(nil, true, durationMilliseconds(metadataTimeout))
	if err != nil {
		slog.Error("failed to connect to confluent kafka server",
			slog.Any("brokers", e.opts.Brokers),
			slog.Any("err", err),
		)
		return err
	}
	listeners := make([]string, 0, len(meta.Brokers))
	for _, broker := range meta.Brokers {
		listeners = append(listeners, fmt.Sprintf("%s:%d", broker.Host, broker.Port))
	}
	slog.Debug("succeeded to connect to confluent kafka server", slog.Any("advertisedListeners", listeners))
	return nil
}

func (e *Ensurer) ensureTopic(ctx context.Context, topic string) error {
	return e.ensureTopicWith(ctx, topic, e.ensureTopicUncached)
}

func (e *Ensurer) ensureTopicWith(ctx context.Context, topic string, ensure func(context.Context, string) error) error {
	e.cacheMu.RLock()
	ready := e.topicsCache[topic]
	e.cacheMu.RUnlock()
	if ready {
		return nil
	}

	e.cacheMu.Lock()
	if e.topicsCache[topic] {
		e.cacheMu.Unlock()
		return nil
	}
	if call, ok := e.topicCalls[topic]; ok {
		e.cacheMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-call.done:
			return call.err
		}
	}
	call := &topicCall{done: make(chan struct{})}
	e.topicCalls[topic] = call
	e.cacheMu.Unlock()

	err := ensure(ctx, topic)

	e.cacheMu.Lock()
	if err == nil {
		e.topicsCache[topic] = true
	}
	call.err = err
	delete(e.topicCalls, topic)
	close(call.done)
	e.cacheMu.Unlock()
	return err
}

func (e *Ensurer) ensureTopicUncached(parent context.Context, topic string) error {
	ctx, cancel := context.WithTimeout(parent, topicEnsureTimeout)
	defer cancel()

	for attempt := 1; attempt <= topicCreateRetries; attempt++ {
		if attempt > 1 && !waitContext(ctx, time.Duration(attempt-1)*100*time.Millisecond) {
			return ctx.Err()
		}

		results, err := e.admin.CreateTopics(ctx, []kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     e.topicOpts.NumPartitions,
			ReplicationFactor: e.topicOpts.ReplicationFactor,
		}})
		if err != nil {
			continue
		}
		if err = topicResultError(results); err != nil {
			continue
		}
		if err = e.waitTopicReady(ctx, topic); err != nil {
			continue
		}

		slog.Debug("successfully ensured confluent kafka topic",
			slog.String("topic", topic),
			slog.Int("partitions", e.topicOpts.NumPartitions),
			slog.Int("replication_factor", e.topicOpts.ReplicationFactor),
		)
		return nil
	}

	err := errors.New("failed to ensure topic exists")
	slog.Error(err.Error(), slog.String("topic", topic))
	return err
}

func topicResultError(results []kafka.TopicResult) error {
	if len(results) != 1 {
		return fmt.Errorf("confluent: unexpected create-topics result count %d", len(results))
	}
	switch results[0].Error.Code() {
	case kafka.ErrNoError, kafka.ErrTopicAlreadyExists:
		return nil
	default:
		return results[0].Error
	}
}

func (e *Ensurer) waitTopicReady(ctx context.Context, topic string) error {
	ticker := time.NewTicker(topicValidateDelay)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for topic %s to be ready: %w", topic, ctx.Err())
		case <-ticker.C:
			meta, err := e.admin.GetMetadata(&topic, false, durationMilliseconds(metadataTimeout))
			if err != nil {
				continue
			}
			topicMeta, ok := meta.Topics[topic]
			if !ok || topicMeta.Error.Code() != kafka.ErrNoError || len(topicMeta.Partitions) == 0 {
				continue
			}
			ready := true
			for _, partition := range topicMeta.Partitions {
				if partition.Error.Code() != kafka.ErrNoError || partition.Leader < 0 || len(partition.Replicas) == 0 {
					ready = false
					break
				}
			}
			if ready {
				return nil
			}
		}
	}
}

func durationMilliseconds(duration time.Duration) int {
	return int(duration / time.Millisecond)
}
