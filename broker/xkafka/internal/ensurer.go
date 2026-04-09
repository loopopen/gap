package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topicEnsureTimeout = 30 * time.Second
	topicCreateRetries = 5
	topicValidateDelay = 500 * time.Millisecond
)

var ensurerOnce sync.Once
var ensurer *Ensurer

type Ensurer struct {
	opts        *Options
	topicOpts   *TopicOptions
	client      *kafka.Client
	topicsCache map[string]bool
	cacheMu     sync.Mutex
}

func SingleEnsurer(opts *Options) *Ensurer {
	ensurerOnce.Do(func() {
		ensurer = &Ensurer{
			opts:        opts,
			topicOpts:   opts.TopicOpts,
			client:      SingleClient(opts),
			topicsCache: make(map[string]bool),
		}
	})
	return ensurer
}

func (e *Ensurer) ensure(ctx context.Context) error {
	_, err := e.client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		slog.Error("kafka: failed to connect to brokers",
			slog.Any("brokers", e.opts.Brokers),
			slog.Any("err", err),
		)
		return err
	}
	return nil
}

// ensureTopic creates a topic if it doesn't exist, with production-grade error handling and retries.
// It uses a cache to avoid redundant create attempts within the same broker instance.
func (e *Ensurer) ensureTopic(ctx context.Context, topic string) error {
	if e.topicsCache[topic] {
		return nil
	}

	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()

	if e.topicsCache[topic] {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, topicEnsureTimeout)
	defer cancel()

	for try := 0; try < topicCreateRetries; try++ {
		if try > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(try) * 100 * time.Millisecond):
			}
		}

		_, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
			Topics: []kafka.TopicConfig{
				{
					Topic:             topic,
					NumPartitions:     e.topicOpts.NumPartitions,
					ReplicationFactor: e.topicOpts.ReplicationFactor,
				},
			},
		})
		if err != nil {
			continue
		}

		err = e.waitTopicReady(ctx, topic)
		if err != nil {
			continue
		}

		// Topic created successfully, mark in cache
		e.topicsCache[topic] = true

		slog.Debug("successfully created kafka topic",
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

// waitTopicReady waits for a topic to be fully replicated and ready for use.
func (e *Ensurer) waitTopicReady(ctx context.Context, topic string) error {
	ticker := time.NewTicker(topicValidateDelay)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for topic %s to be ready: %w", topic, ctx.Err())
		case <-ticker.C:
			// Fetch metadata for the specific topic using the client.
			// Unlike Conn, the Client manages its own connection pool via Transport.
			resp, err := e.client.Metadata(ctx, &kafka.MetadataRequest{
				Topics: []string{topic},
			})

			if err != nil {
				// Network errors or broker unavailability; retry in the next tick
				continue
			}

			for _, t := range resp.Topics {
				if t.Name != topic {
					continue
				}

				// If the topic has metadata errors (e.g., LeaderNotAvailable)
				// or no partitions exist yet, it is not ready.
				if t.Error != nil || len(t.Partitions) == 0 {
					break
				}

				allReady := true
				for _, p := range t.Partitions {
					// A partition is considered 'ready' when:
					// 1. It has a valid Leader (ID >= 0)
					// 2. The replica list is populated
					if p.Leader.ID < 0 || len(p.Replicas) == 0 {
						allReady = false
						break
					}
				}

				if allReady {
					return nil
				}
			}
		}
	}
}
