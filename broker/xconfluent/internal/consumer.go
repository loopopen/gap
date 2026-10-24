package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/loopopen/gap/v2/broker"
	"github.com/loopopen/gap/v2/spi/entity"
	internalgap "github.com/loopopen/gap/v2/spi/runtime"
)

var (
	errInvalidDeliveryTag       = errors.New("confluent: invalid delivery tag")
	errDeliverySettled          = errors.New("confluent: delivery already settled")
	errConsumerAlreadyConsuming = errors.New("confluent: consumer has already started consuming")
	errConsumerClosed           = errors.New("confluent: consumer is closed")
	errSubscribeStarted         = errors.New("confluent: cannot subscribe after consuming has started")
)

const (
	defaultPollTimeout    = 250 * time.Millisecond
	defaultReadRetryDelay = 5 * time.Second
	defaultCommitRetryMin = 100 * time.Millisecond
	defaultCommitRetryMax = 5 * time.Second
)

type consumerClient interface {
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	CommitMessage(message *kafka.Message) ([]kafka.TopicPartition, error)
	Assignment() ([]kafka.TopicPartition, error)
	Assign(partitions []kafka.TopicPartition) error
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
	group   string
	groupID string
	ensurer *Ensurer

	mu       sync.Mutex
	state    consumerState
	topics   []string
	consumer consumerClient

	newConsumer    func() (consumerClient, error)
	pollTimeout    time.Duration
	readRetryDelay time.Duration
	commitRetryMin time.Duration
	commitRetryMax time.Duration
}

func NewConsumer(gapOpts *internalgap.Options, opts *Options, group string) (*Consumer, error) {
	if group == "" {
		panic("group should not be empty")
	}
	ensurer, err := SingleEnsurer(opts)
	if err != nil {
		return nil, err
	}
	groupID := fmt.Sprintf("gap.%s.g.%s", gapOpts.Version, group)
	consumer := &Consumer{
		gapOpts:        gapOpts,
		opts:           opts,
		group:          group,
		groupID:        groupID,
		ensurer:        ensurer,
		state:          consumerCreated,
		pollTimeout:    defaultPollTimeout,
		readRetryDelay: defaultReadRetryDelay,
		commitRetryMin: defaultCommitRetryMin,
		commitRetryMax: defaultCommitRetryMax,
	}
	consumer.newConsumer = func() (consumerClient, error) {
		config, err := consumerConfig(opts, groupID)
		if err != nil {
			return nil, err
		}
		return kafka.NewConsumer(&config)
	}
	return consumer, nil
}

func (c *Consumer) init() error {
	return c.ensurer.ensure(c.gapOpts.Context)
}

func (c *Consumer) Subscribe(ctx context.Context, topic string) error {
	c.mu.Lock()
	err := c.subscribeStateError()
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if err = c.ensurer.ensureTopic(ctx, topic); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err = c.subscribeStateError(); err != nil {
		return err
	}
	if slices.Contains(c.topics, topic) {
		return nil
	}
	c.topics = append(c.topics, topic)
	c.state = consumerSubscribed
	slog.Debug("subscribed to confluent kafka topic", slog.String("topic", topic), slog.String("group", c.group))
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

func (c *Consumer) Consume(ctx context.Context) (<-chan *entity.Envelope, error) {
	c.mu.Lock()
	if len(c.topics) == 0 {
		c.mu.Unlock()
		return nil, errors.New("no topics subscribed")
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
	client, err := c.newConsumer()
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if err = client.SubscribeTopics(topics, nil); err != nil {
		c.mu.Unlock()
		_ = client.Close()
		return nil, err
	}
	c.consumer = client
	c.state = consumerRunning
	c.mu.Unlock()

	envelopes := make(chan *entity.Envelope)
	go c.consumeLoop(ctx, client, envelopes)
	return envelopes, nil
}

func (c *Consumer) consumeLoop(ctx context.Context, client consumerClient, envelopes chan<- *entity.Envelope) {
	defer close(envelopes)
	defer func() {
		if err := client.Close(); err != nil {
			slog.Error("confluent: close consumer failed", slog.Any("err", err))
		}
		c.mu.Lock()
		c.consumer = nil
		c.state = consumerClosed
		c.mu.Unlock()
	}()

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		message, err := client.ReadMessage(c.pollTimeout)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			slog.Error("confluent: read message failed", slog.Any("err", err))
			if !waitContext(ctx, c.readRetryDelay) {
				return
			}
			continue
		}
		if message == nil || message.TopicPartition.Topic == nil {
			slog.Error("confluent: received message without topic")
			continue
		}

		envelope := c.envelope(message)
		tag := newDeliveryTag(ctx, c)
		envelope.Tag = tag
		select {
		case <-ctx.Done():
			return
		case envelopes <- envelope:
		}

		action, ok := waitDeliveryAction(ctx, tag)
		if !ok {
			return
		}
		switch action.kind {
		case deliveryCommit:
			err = c.commitWithRetry(ctx, client, message)
		case deliveryReject:
			err = c.reassign(client, message)
		}
		action.done <- err
		if err != nil {
			return
		}
	}
}

func (c *Consumer) envelope(message *kafka.Message) *entity.Envelope {
	topic := *message.TopicPartition.Topic
	envelope := entity.NewEnvelope(c.gapOpts.Version, topic, nil).
		WithPayload(message.Value).
		WithGroup(c.group)
	for _, header := range message.Headers {
		envelope.AddHeader(header.Key, string(header.Value))
	}
	envelope.Log().Debug("confluent: received a message",
		slog.Int("partition", int(message.TopicPartition.Partition)),
		slog.Int64("offset", int64(message.TopicPartition.Offset)),
	)
	return envelope
}

func (c *Consumer) commitWithRetry(ctx context.Context, client consumerClient, message *kafka.Message) error {
	delay := c.commitRetryMin
	for attempt := 1; ; attempt++ {
		_, err := client.CommitMessage(message)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("confluent: commit message failed; retrying",
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

// reassign follows dotnet CAP's reject strategy: reapply the current
// assignment without committing the rejected message. OffsetStored makes each
// partition resume from its committed group offset.
func (c *Consumer) reassign(client consumerClient, message *kafka.Message) error {
	assignment, err := client.Assignment()
	if err != nil {
		return err
	}
	if len(assignment) == 0 {
		return errors.New("confluent: cannot reject without a partition assignment")
	}
	for index := range assignment {
		assignment[index].Offset = kafka.OffsetStored
	}
	if err = client.Assign(assignment); err != nil {
		return err
	}
	slog.Warn("confluent: message rejected; reset current assignment to committed offsets",
		slog.String("topic", *message.TopicPartition.Topic),
		slog.Int("partition", int(message.TopicPartition.Partition)),
		slog.Int64("offset", int64(message.TopicPartition.Offset)),
		slog.String("group_id", c.groupID),
	)
	return nil
}

func (c *Consumer) Commit(tag any) error {
	delivery, err := c.deliveryTag(tag)
	if err != nil {
		return err
	}
	return delivery.submit(deliveryCommit)
}

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

func isTimeout(err error) bool {
	var kafkaError kafka.Error
	return errors.As(err, &kafkaError) && kafkaError.IsTimeout()
}

var _ broker.Consumer = (*Consumer)(nil)
