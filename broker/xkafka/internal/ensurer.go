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

var ensurerOnce sync.Once
var ensurer *Ensurer

type Ensurer struct {
	connFactory *ConnFactory
	topicOpts   *TopicOptions
	topicsCache map[string]bool
	cacheMu     sync.Mutex
}

func SingleEnsurer(opts *Options) *Ensurer {
	ensurerOnce.Do(func() {
		ensurer = &Ensurer{
			connFactory: NewConnFactory(opts),
			topicOpts:   opts.TopicOpts,
			topicsCache: make(map[string]bool),
		}
	})
	return ensurer
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

	for try := 0; try < topicCreateRetries; try++ {
		if try > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(try) * 100 * time.Millisecond):
			}
		}

		// Try each broker until one succeeds
		conn, err := e.connFactory.CreateConn(true)
		if err != nil {
			continue
		}

		err = conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     e.topicOpts.NumPartitions,
			ReplicationFactor: e.topicOpts.ReplicationFactor,
		})
		conn.Close()
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
	const maxWaitTime = 30 * time.Second
	ctx, cancel := context.WithTimeout(ctx, maxWaitTime)
	defer cancel()

	ticker := time.NewTicker(topicValidateDelay)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for topic to be ready: %w", ctx.Err())
		case <-ticker.C:
			conn, err := e.connFactory.CreateConn(false)
			if err != nil {
				continue
			}

			partitions, err := conn.ReadPartitions(topic)
			conn.Close()

			if err == nil && len(partitions) > 0 {
				// Verify all replicas are available
				allReady := true
				for _, partition := range partitions {
					// Check if leader is valid (Leader.ID should be >= 0)
					if len(partition.Replicas) == 0 || partition.Leader.ID < 0 {
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
