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
	"github.com/lopolopen/gap/internal/gap"
	"github.com/segmentio/kafka-go"
)

type Reader struct {
	gapOpts  *gap.Options
	opts     *Options
	client   *kafka.Client
	group    string
	reader   *kafka.Reader
	readerMu sync.Mutex
	groupID  string
	topics   []string
	topicMu  sync.Mutex
	ensurer  *Ensurer
}

// NewReader creates a new Kafka reader broker instance.
func NewReader(gapOpts *gap.Options, group string) *Reader {
	if group == "" {
		panic("group should not be empty")
	}

	opts := gapOpts.BrokerOptions.(*Options)

	reader := &Reader{
		gapOpts: gapOpts,
		opts:    opts,
		client:  SingleClient(opts),
		groupID: fmt.Sprintf("gap.%s.g.%s", gapOpts.Version, group),
		group:   group,
		ensurer: SingleEnsurer(opts),
	}

	var _ broker.Reader = reader
	return reader
}

func (r *Reader) init() error {
	return r.ensurer.ensure(r.gapOpts.Context)
}

// Subscribe implements [gap.Reader].
func (r *Reader) Subscribe(ctx context.Context, topic string) error {
	if err := r.ensurer.ensureTopic(ctx, topic); err != nil {
		return err
	}

	r.topicMu.Lock()
	defer r.topicMu.Unlock()

	if slices.Contains(r.topics, topic) {
		return nil
	}

	r.topics = append(r.topics, topic)
	slog.Debug(fmt.Sprintf("subscribed to topic: %s (%s)", topic, r.group))
	return nil
}

func (r *Reader) renewReader() {
	r.readerMu.Lock()
	defer r.readerMu.Unlock()

	if r.reader != nil {
		r.reader.Close()
	}
	r.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:          r.opts.Brokers,
		GroupID:          r.groupID,
		GroupTopics:      r.topics,
		StartOffset:      r.opts.StartOffset,
		CommitInterval:   0,
		ReadBatchTimeout: 3 * time.Second,
	})
}

// Read implements [gap.Reader].
func (r *Reader) Read(ctx context.Context) (<-chan *entity.Envelope, error) {
	if len(r.topics) == 0 {
		return nil, fmt.Errorf("no topics subscribed")
	}

	r.renewReader()

	enCh := make(chan *entity.Envelope)

	go func() {
		defer close(enCh)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msg, err := r.reader.FetchMessage(ctx)
			slog.Debug("kafka: fetched a message",
				slog.String("topic", msg.Topic),
				slog.Int64("offset", msg.Offset),
				slog.String("group_id", r.groupID),
			)

			if err != nil {
				if !errors.Is(err, context.Canceled) {
					slog.Error("kafka: read message failed", slog.Any("err", err))
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
					continue
				}
			}

			en := entity.NewEnvelope(r.gapOpts.Version, msg.Topic, nil).
				WithPayload(msg.Value).
				WithGroup(r.group).
				WithTag(msg)

			for _, h := range msg.Headers {
				en.AddHeader(h.Key, string(h.Value))
			}

			en.Log().Debug("kafka: received a message")

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
func (r *Reader) Commit(tag any) error {
	msg := tag.(kafka.Message)
	slog.Debug("kafka: commit a message",
		slog.String("topic", msg.Topic),
		slog.Int64("offset", msg.Offset),
	)
	return r.reader.CommitMessages(context.Background(), msg)
}

// Reject implements [gap.Reader].
func (r *Reader) Reject(tag any) error {
	r.renewReader()
	return nil
}
