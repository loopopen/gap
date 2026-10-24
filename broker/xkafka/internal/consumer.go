package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/loopopen/gap/v2/broker"
	"github.com/loopopen/gap/v2/spi/entity"
	internalgap "github.com/loopopen/gap/v2/spi/runtime"
	"github.com/segmentio/kafka-go"
)

var (
	errInvalidDeliveryTag       = errors.New("kafka: invalid delivery tag")
	errDeliverySettled          = errors.New("kafka: delivery already settled")
	errConsumerAlreadyConsuming = errors.New("kafka: reader has already started consuming")
	errConsumerClosed           = errors.New("kafka: consumer is closed")
	errSubscribeStarted         = errors.New("kafka: cannot subscribe after reading has started")
)

const (
	defaultReadRetryDelay   = 5 * time.Second
	defaultRejectRetryDelay = time.Second
	defaultCommitRetryMin   = 100 * time.Millisecond
	defaultCommitRetryMax   = 5 * time.Second
)

type kafkaReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

type consumerState uint8

const (
	consumerCreated consumerState = iota
	consumerSubscribed
	consumerRunning
	consumerClosed
)

type deliveryActionKind uint8

const (
	deliveryCommit deliveryActionKind = iota
	deliveryReject
)

type deliveryAction struct {
	kind deliveryActionKind
	done chan error
}

// deliveryTag binds an acknowledgement to one delivery attempt and one Consumer.
// A rejected message gets a new tag before redelivery, so a late acknowledgement
// from an older attempt cannot settle a newer attempt.
type deliveryTag struct {
	owner   *Consumer
	ctx     context.Context
	actions chan deliveryAction

	mu      sync.Mutex
	settled bool
}

func newDeliveryTag(ctx context.Context, owner *Consumer) *deliveryTag {
	return &deliveryTag{owner: owner, ctx: ctx, actions: make(chan deliveryAction)}
}

func (t *deliveryTag) submit(kind deliveryActionKind) error {
	t.mu.Lock()
	if t.settled {
		t.mu.Unlock()
		return errDeliverySettled
	}
	t.settled = true
	t.mu.Unlock()

	action := deliveryAction{kind: kind, done: make(chan error, 1)}
	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	case t.actions <- action:
	}

	select {
	case <-t.ctx.Done():
		return t.ctx.Err()
	case err := <-action.done:
		return err
	}
}

type Consumer struct {
	gapOpts *internalgap.Options
	opts    *Options
	client  *kafka.Client
	dialer  *kafka.Dialer
	group   string
	groupID string
	ensurer *Ensurer

	mu     sync.Mutex
	state  consumerState
	topics []string
	reader kafkaReader

	newReader        func([]string) kafkaReader
	readRetryDelay   time.Duration
	rejectRetryDelay time.Duration
	commitRetryMin   time.Duration
	commitRetryMax   time.Duration
}

// NewConsumer creates a new Kafka consumer broker instance.
func NewConsumer(gapOpts *internalgap.Options, opts *Options, group string) *Consumer {
	if group == "" {
		panic("group should not be empty")
	}

	dialer := SingleDialer(opts)
	groupID := fmt.Sprintf("gap.%s.g.%s", gapOpts.Version, group)

	c := &Consumer{
		gapOpts:          gapOpts,
		opts:             opts,
		client:           SingleClient(opts),
		dialer:           dialer,
		groupID:          groupID,
		group:            group,
		ensurer:          SingleEnsurer(opts),
		state:            consumerCreated,
		readRetryDelay:   defaultReadRetryDelay,
		rejectRetryDelay: defaultRejectRetryDelay,
		commitRetryMin:   defaultCommitRetryMin,
		commitRetryMax:   defaultCommitRetryMax,
	}
	c.newReader = func(topics []string) kafkaReader {
		return newKafkaReader(opts, dialer, groupID, topics)
	}

	var _ broker.Consumer = c
	return c
}

func (c *Consumer) init() error {
	return c.ensurer.ensure(c.gapOpts.Context)
}

// Subscribe implements [broker.Consumer]. Subscriptions are immutable after Consume
// starts so the running reader and the consumer topic set cannot diverge.
func (c *Consumer) Subscribe(ctx context.Context, topic string) error {
	c.mu.Lock()
	err := c.subscribeStateError()
	c.mu.Unlock()
	if err != nil {
		return err
	}

	if err := c.ensurer.ensureTopic(ctx, topic); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.subscribeStateError(); err != nil {
		return err
	}
	if slices.Contains(c.topics, topic) {
		return nil
	}
	c.topics = append(c.topics, topic)
	c.state = consumerSubscribed
	slog.Debug(fmt.Sprintf("subscribed to topic: %s (%s)", topic, c.group))
	return nil
}

func (c *Consumer) subscribeStateError() error {
	switch c.state {
	case consumerRunning:
		return errSubscribeStarted
	case consumerClosed:
		return errConsumerClosed
	default:
		return nil
	}
}

func newKafkaReader(opts *Options, dialer *kafka.Dialer, groupID string, topics []string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:          opts.Brokers,
		Dialer:           dialer,
		GroupID:          groupID,
		GroupTopics:      topics,
		StartOffset:      opts.StartOffset,
		CommitInterval:   0,
		ReadBatchTimeout: 3 * time.Second,
	})
}

// Consume implements [broker.Consumer]. Only one message may be unsettled at a time.
// Reject redelivers that message in memory instead of replacing the consumer and
// forcing the consumer-group member to leave and rejoin.
func (c *Consumer) Consume(ctx context.Context) (<-chan *entity.Envelope, error) {
	c.mu.Lock()
	if len(c.topics) == 0 {
		c.mu.Unlock()
		return nil, fmt.Errorf("no topics subscribed")
	}
	switch c.state {
	case consumerRunning:
		c.mu.Unlock()
		return nil, errConsumerAlreadyConsuming
	case consumerClosed:
		c.mu.Unlock()
		return nil, errConsumerClosed
	}

	topics := slices.Clone(c.topics)
	reader := c.newReader(topics)
	c.reader = reader
	c.state = consumerRunning
	c.mu.Unlock()

	enCh := make(chan *entity.Envelope)
	go c.readLoop(ctx, reader, enCh)
	return enCh, nil
}

func (c *Consumer) readLoop(ctx context.Context, reader kafkaReader, enCh chan<- *entity.Envelope) {
	defer close(enCh)
	defer func() {
		if err := reader.Close(); err != nil {
			slog.Error("kafka: close reader failed", slog.Any("err", err))
		}
		c.mu.Lock()
		c.reader = nil
		c.state = consumerClosed
		c.mu.Unlock()
	}()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("kafka: read message failed", slog.Any("err", err))
			if !waitContext(ctx, c.readRetryDelay) {
				return
			}
			continue
		}

		slog.Debug("kafka: fetched a message",
			slog.String("topic", msg.Topic),
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
			slog.String("group_id", c.groupID),
		)

		envelope := c.envelope(msg)
		for {
			tag := newDeliveryTag(ctx, c)
			attempt := *envelope
			attempt.Tag = tag
			select {
			case <-ctx.Done():
				return
			case enCh <- &attempt:
			}

			action, ok := waitDeliveryAction(ctx, tag)
			if !ok {
				return
			}
			switch action.kind {
			case deliveryCommit:
				err = c.commitWithRetry(ctx, reader, msg)
				action.done <- err
				if err != nil {
					return
				}
				goto next
			case deliveryReject:
				slog.Warn("kafka: message rejected; retrying without restarting consumer",
					slog.String("topic", msg.Topic),
					slog.Int("partition", msg.Partition),
					slog.Int64("offset", msg.Offset),
					slog.String("group_id", c.groupID),
				)
				action.done <- nil
				if !waitContext(ctx, c.rejectRetryDelay) {
					return
				}
			}
		}

	next:
	}
}

func (c *Consumer) envelope(msg kafka.Message) *entity.Envelope {
	en := entity.NewEnvelope(c.gapOpts.Version, msg.Topic, nil).
		WithPayload(msg.Value).
		WithGroup(c.group)
	for _, h := range msg.Headers {
		en.AddHeader(h.Key, string(h.Value))
	}
	en.Log().Debug("kafka: received a message",
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
	)
	return en
}

func waitDeliveryAction(ctx context.Context, tag *deliveryTag) (deliveryAction, bool) {
	select {
	case <-ctx.Done():
		return deliveryAction{}, false
	case action := <-tag.actions:
		return action, true
	}
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (c *Consumer) commitWithRetry(ctx context.Context, reader kafkaReader, msg kafka.Message) error {
	delay := c.commitRetryMin
	for attempt := 1; ; attempt++ {
		err := reader.CommitMessages(ctx, msg)
		if err == nil {
			slog.Debug("kafka: committed a message",
				slog.String("topic", msg.Topic),
				slog.Int("partition", msg.Partition),
				slog.Int64("offset", msg.Offset),
				slog.String("group_id", c.groupID),
			)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		slog.Warn("kafka: commit message failed; retrying",
			slog.String("topic", msg.Topic),
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
			slog.Int("attempt", attempt),
			slog.Duration("retry_in", delay),
			slog.Any("err", err),
		)
		if !waitContext(ctx, delay) {
			return ctx.Err()
		}
		if delay < c.commitRetryMax {
			delay *= 2
			if delay > c.commitRetryMax {
				delay = c.commitRetryMax
			}
		}
	}
}

// Commit implements [broker.Consumer]. The read loop owns the actual Kafka
// operation so fetch, retry, commit, and close share one Consumer lifecycle.
func (c *Consumer) Commit(tag any) error {
	delivery, err := c.deliveryTag(tag)
	if err != nil {
		return err
	}
	return delivery.submit(deliveryCommit)
}

// Reject implements [broker.Consumer]. Kafka has no per-message nack; rejection
// keeps the current message in memory and redelivers it after a bounded delay.
func (c *Consumer) Reject(tag any) error {
	delivery, err := c.deliveryTag(tag)
	if err != nil {
		return err
	}
	return delivery.submit(deliveryReject)
}

func (c *Consumer) deliveryTag(tag any) (*deliveryTag, error) {
	delivery, ok := tag.(*deliveryTag)
	if !ok || delivery == nil || delivery.owner != c {
		return nil, fmt.Errorf("%w: got %T", errInvalidDeliveryTag, tag)
	}
	return delivery, nil
}
