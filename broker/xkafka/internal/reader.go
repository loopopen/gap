package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/options/gap"
	"github.com/segmentio/kafka-go"
)

const (
// // Topic creation default timeouts
// topicCreateTimeout = 30 * time.Second
// topicCreateRetries = 5

// // Topic validation retry backoff
// topicValidateDelay = 500 * time.Millisecond

// corrid = internal.KeysCorrelationID
)

type Reader struct {
	gapOpts     *gap.Options
	opts        *Options
	group       string
	connFactory *ConnFactory
	reader      *kafka.Reader
	readerMu    sync.Mutex
	groupID     string
	topics      []string
	topicMu     sync.Mutex
	ensurer     *Ensurer
}

// NewReader creates a new Kafka reader broker instance.
func NewReader(gapOpts *gap.Options, group string) *Reader {
	if group == "" {
		panic("group should not be empty")
	}

	bp := gapOpts.BrokerPlugin
	opts := bp.(*Options)

	reader := &Reader{
		gapOpts:     gapOpts,
		opts:        opts,
		connFactory: NewConnFactory(opts),
		groupID:     fmt.Sprintf("gap.%s.g.%s", gapOpts.Version, group),
		group:       group,
		ensurer:     SingleEnsurer(opts),
	}

	var _ broker.Reader = reader
	return reader
}

func (b *Reader) init() error {
	_, err := b.connFactory.CreateConn(false)
	if err != nil {
		return err
	}
	return nil
}

// Subscribe implements [gap.Reader].
func (b *Reader) Subscribe(ctx context.Context, topic string) error {
	ctx, cancel := context.WithTimeout(ctx, topicCreateTimeout)
	defer cancel()

	if err := b.ensurer.ensureTopic(ctx, topic); err != nil {
		return err
	}

	b.topicMu.Lock()
	defer b.topicMu.Unlock()

	if slices.Contains(b.topics, topic) {
		return nil
	}

	b.topics = append(b.topics, topic)
	slog.Debug(fmt.Sprintf("subscribed to topic: %s (%s)", topic, b.group))
	return nil
}

func (b *Reader) renewReader() {
	b.readerMu.Lock()
	defer b.readerMu.Unlock()

	if b.reader != nil {
		b.reader.Close()
	}
	b.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:          b.opts.Brokers,
		GroupID:          b.groupID,
		GroupTopics:      b.topics,
		StartOffset:      b.opts.StartOffset,
		CommitInterval:   0,
		ReadBatchTimeout: 3 * time.Second,
	})
}

// Read implements [gap.Reader].
func (b *Reader) Read(ctx context.Context) (<-chan *entity.Envelope, error) {
	if len(b.topics) == 0 {
		return nil, fmt.Errorf("no topics subscribed")
	}

	b.renewReader()

	enCh := make(chan *entity.Envelope)

	go func() {
		defer close(enCh)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msg, err := b.reader.FetchMessage(ctx)
			slog.Debug("fetched a message from kafka",
				slog.String("topic", msg.Topic),
				slog.Int64("offset", msg.Offset),
				slog.String("group.id", b.groupID),
			)

			if err != nil {
				if !errors.Is(err, context.Canceled) {
					slog.Error("broker read message failed", slog.Any("err", err))
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
					continue
				}
			}

			en := entity.NewEnvelope(b.gapOpts.Version, msg.Topic, nil).
				WithPayload(msg.Value).
				WithGroup(b.group).
				WithTag(msg)

			for _, h := range msg.Headers {
				en.AddHeader(h.Key, string(h.Value))
			}

			slog.Debug("kafka: received a message",
				slog.String("topic", en.Topic),
				slog.String("id", en.IDString()),
				slog.String(corrid, en.Headers[corrid]),
			)

			select {
			case <-ctx.Done():
				return
			case enCh <- en:
			}
		}
	}()

	return enCh, nil
}

// Commit implements [gap.Reader].
func (b *Reader) Commit(tag any) error {
	msg := tag.(kafka.Message)
	slog.Debug("kafka: commit a message",
		slog.String("topic", msg.Topic),
		slog.Int64("offset", msg.Offset),
	)
	return b.reader.CommitMessages(context.Background(), msg)
}

// Reject implements [gap.Reader].
func (b *Reader) Reject(tag any) error {
	b.renewReader()
	return nil
}
