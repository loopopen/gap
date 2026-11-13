package internal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
)

var _ broker.Closer = (*Consumer)(nil)
var _ broker.Consumer = (*Consumer)(nil)

var (
	errInvalidDeliveryTag       = errors.New("redisstreams: invalid delivery tag")
	errDeliverySettled          = errors.New("redisstreams: delivery already settled")
	errConsumerAlreadyConsuming = errors.New("redisstreams: consumer has already started consuming")
	errConsumerClosed           = errors.New("redisstreams: consumer is closed")
	errSubscribeStarted         = errors.New("redisstreams: cannot subscribe after consuming has started")
	errNoTopics                 = errors.New("redisstreams: no topics subscribed")
)

const (
	defaultReadRetryDelay   = 5 * time.Second
	defaultRejectRetryDelay = time.Second
	defaultCommitRetryMin   = 100 * time.Millisecond
	defaultCommitRetryMax   = 5 * time.Second
	defaultClaimMinIdle     = 5 * time.Second
	maxClaimPages           = 8
)

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

// deliveryTag binds an acknowledgement to one delivery attempt. A rejected
// message gets a new tag before redelivery, so a late acknowledgement from an
// older attempt cannot settle a newer attempt.
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

type inflightSet struct {
	mu  sync.Mutex
	ids map[string]struct{}
}

func (s *inflightSet) tryAdd(stream, id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids == nil {
		s.ids = make(map[string]struct{})
	}
	key := stream + "\x00" + id
	if _, ok := s.ids[key]; ok {
		return false
	}
	s.ids[key] = struct{}{}
	return true
}

func (s *inflightSet) remove(stream, id string) {
	s.mu.Lock()
	delete(s.ids, stream+"\x00"+id)
	s.mu.Unlock()
}

// Consumer reads a Redis consumer group.
//
// Pending entries for this consumer are drained before new ones. Commit is
// XACK. Reject leaves the entry pending and redelivers it in memory. Idle
// entries owned by a dead consumer are reclaimed with XAUTOCLAIM so a
// restarted process still receives them.
type Consumer struct {
	gapOpts  *ingap.Options
	opts     *Options
	client   streamClient
	group    string
	groupID  string
	consumer string
	startID  string

	mu     sync.Mutex
	state  consumerState
	topics []string
	cancel context.CancelFunc

	inflight inflightSet

	readRetryDelay   time.Duration
	rejectRetryDelay time.Duration
	commitRetryMin   time.Duration
	commitRetryMax   time.Duration
	claimMinIdle     time.Duration
	blockTimeout     time.Duration
	prefetch         int
}

func NewConsumer(gapOpts *ingap.Options, opts *Options, group string) (*Consumer, error) {
	if group == "" {
		panic("group should not be empty")
	}
	client, err := SingleClient(opts)
	if err != nil {
		return nil, err
	}
	groupID := fmt.Sprintf("gap.%s.g.%s", gapOpts.Version, group)
	return &Consumer{
		gapOpts:          gapOpts,
		opts:             opts,
		client:           client,
		group:            group,
		groupID:          groupID,
		consumer:         newConsumerName(groupID),
		startID:          opts.groupStart(),
		readRetryDelay:   defaultReadRetryDelay,
		rejectRetryDelay: defaultRejectRetryDelay,
		commitRetryMin:   defaultCommitRetryMin,
		commitRetryMax:   defaultCommitRetryMax,
		claimMinIdle:     defaultClaimMinIdle,
		blockTimeout:     opts.blockTimeout(),
		prefetch:         int(opts.entriesCount()),
	}, nil
}

func newConsumerName(groupID string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "local"
	}
	var buf [4]byte
	if _, err = rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%s.%s.%d", groupID, host, os.Getpid())
	}
	return fmt.Sprintf("%s.%s.%d.%s", groupID, host, os.Getpid(), hex.EncodeToString(buf[:]))
}

func (c *Consumer) init() error {
	return c.client.Ping(c.gapOpts.IntakeContext())
}

// Subscribe implements [broker.Consumer]. The consumer group is created when
// the topic is subscribed.
func (c *Consumer) Subscribe(ctx context.Context, topic string) error {
	if topic == "" {
		return errx.Permanent(errx.ErrEmptyTopic)
	}
	c.mu.Lock()
	err := c.subscribeStateError()
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if err = c.client.EnsureGroup(ctx, topic, c.groupID, c.startID); err != nil {
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
	slog.Debug("redisstreams: subscribed to stream", slog.String("stream", topic), slog.String("group", c.groupID))
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

// Consume implements [broker.Consumer]. Pending entries are delivered and
// settled before new entries. Several new entries may be unsettled together,
// bounded by StreamEntriesCount.
func (c *Consumer) Consume(ctx context.Context) (<-chan *entity.Envelope, error) {
	c.mu.Lock()
	if len(c.topics) == 0 {
		c.mu.Unlock()
		return nil, errNoTopics
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
	readCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.state = consumerRunning
	c.mu.Unlock()

	enCh := make(chan *entity.Envelope)
	go c.readLoop(readCtx, topics, enCh)
	return enCh, nil
}

func (c *Consumer) readLoop(intakeCtx context.Context, topics []string, enCh chan<- *entity.Envelope) {
	if c.prefetch < 1 {
		c.prefetch = defaultStreamEntriesCount
	}
	if c.blockTimeout <= 0 {
		c.blockTimeout = defaultBlockTimeout
	}

	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		close(enCh)
		c.mu.Lock()
		c.state = consumerClosed
		c.mu.Unlock()
	}()

	c.drainPending(intakeCtx, topics, enCh)

	sem := make(chan struct{}, c.prefetch)
	for {
		if intakeCtx.Err() != nil {
			return
		}
		c.spawnDeliveries(intakeCtx, enCh, &wg, sem, c.claimIdle(intakeCtx, topics))
		if intakeCtx.Err() != nil {
			return
		}

		entries, err := c.readNew(intakeCtx, topics)
		if err != nil {
			if intakeCtx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			slog.Error("redisstreams: read message failed", slog.Any("err", err))
			if !waitContext(intakeCtx, c.readRetryDelay) {
				return
			}
			continue
		}
		c.spawnDeliveries(intakeCtx, enCh, &wg, sem, entries)
	}
}

func (c *Consumer) drainPending(intakeCtx context.Context, topics []string, enCh chan<- *entity.Envelope) {
	for {
		if intakeCtx.Err() != nil {
			return
		}
		batch, err := c.client.ReadGroup(intakeCtx, c.groupID, c.consumer, topics, readPending, int64(c.prefetch), -1)
		if err != nil {
			if intakeCtx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			if isNoGroup(err) {
				if ensureErr := c.ensureTopics(intakeCtx, topics); ensureErr != nil {
					slog.Error("redisstreams: recreate consumer group failed", slog.Any("err", ensureErr))
				}
			} else {
				slog.Error("redisstreams: read pending messages failed", slog.Any("err", err))
			}
			return
		}
		delivered := 0
		for _, entry := range batch {
			if intakeCtx.Err() != nil {
				return
			}
			if !c.inflight.tryAdd(entry.Stream, entry.ID) {
				continue
			}
			delivered++
			c.deliver(intakeCtx, enCh, entry)
		}
		if delivered == 0 || len(batch) < c.prefetch {
			return
		}
	}
}

func (c *Consumer) readNew(ctx context.Context, topics []string) ([]streamEntry, error) {
	entries, err := c.client.ReadGroup(ctx, c.groupID, c.consumer, topics, readNew, int64(c.prefetch), c.blockTimeout)
	if err != nil && isNoGroup(err) && ctx.Err() == nil {
		if ensureErr := c.ensureTopics(ctx, topics); ensureErr != nil {
			return nil, ensureErr
		}
		return c.client.ReadGroup(ctx, c.groupID, c.consumer, topics, readNew, int64(c.prefetch), c.blockTimeout)
	}
	return entries, err
}

func (c *Consumer) ensureTopics(ctx context.Context, topics []string) error {
	for _, topic := range topics {
		if err := c.client.EnsureGroup(ctx, topic, c.groupID, c.startID); err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) claimIdle(ctx context.Context, topics []string) []streamEntry {
	if c.claimMinIdle <= 0 {
		return nil
	}
	var claimed []streamEntry
	for _, topic := range topics {
		cursor := claimStart
		for page := 0; page < maxClaimPages; page++ {
			if ctx.Err() != nil {
				return claimed
			}
			entries, next, err := c.client.AutoClaim(ctx, topic, c.groupID, c.consumer, cursor, c.claimMinIdle, int64(c.prefetch))
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
					return claimed
				}
				if claimUnsupported(err) {
					slog.Warn("redisstreams: idle claim is unavailable; pending entries are recovered on startup", slog.Any("err", err))
					c.claimMinIdle = 0
					return claimed
				}
				slog.Warn("redisstreams: claim idle messages failed", slog.String("stream", topic), slog.Any("err", err))
				break
			}
			claimed = append(claimed, entries...)
			if len(entries) == 0 || next == "" || next == claimStart || next == cursor {
				break
			}
			cursor = next
		}
	}
	return claimed
}

func (c *Consumer) spawnDeliveries(intakeCtx context.Context, enCh chan<- *entity.Envelope, wg *sync.WaitGroup, sem chan struct{}, entries []streamEntry) {
	for _, entry := range entries {
		if !c.inflight.tryAdd(entry.Stream, entry.ID) {
			continue
		}
		select {
		case <-intakeCtx.Done():
			c.inflight.remove(entry.Stream, entry.ID)
			return
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(entry streamEntry) {
			defer wg.Done()
			defer func() { <-sem }()
			c.deliver(intakeCtx, enCh, entry)
		}(entry)
	}
}

func (c *Consumer) deliver(intakeCtx context.Context, enCh chan<- *entity.Envelope, entry streamEntry) {
	defer c.inflight.remove(entry.Stream, entry.ID)

	headers, body, err := entryMessage(entry.Values)
	if err != nil {
		slog.Error("redisstreams: acknowledging undecodable entry so the group can move forward",
			slog.String("stream", entry.Stream),
			slog.String("id", entry.ID),
			slog.Any("err", err),
		)
		if ackErr := c.ackWithRetry(c.deliveryContext(intakeCtx), entry); ackErr != nil {
			slog.Error("redisstreams: failed to acknowledge undecodable entry", slog.Any("err", ackErr))
		}
		return
	}

	envelope := c.envelope(entry, headers, body)
	for {
		deliveryCtx := c.deliveryContext(intakeCtx)
		tag := newDeliveryTag(deliveryCtx, c)
		attempt := *envelope
		attempt.Tag = tag
		select {
		case <-intakeCtx.Done():
			return
		case enCh <- &attempt:
		}

		action, ok := waitDeliveryAction(deliveryCtx, tag)
		if !ok {
			return
		}
		switch action.kind {
		case deliveryCommit:
			err = c.ackWithRetry(deliveryCtx, entry)
			action.done <- err
			return
		case deliveryReject:
			slog.Warn("redisstreams: message rejected; retrying without acknowledging",
				slog.String("stream", entry.Stream),
				slog.String("id", entry.ID),
				slog.String("group", c.groupID),
			)
			action.done <- nil
			if !waitContext(deliveryCtx, c.rejectRetryDelay) {
				return
			}
		}
	}
}

func (c *Consumer) envelope(entry streamEntry, headers map[string]string, body []byte) *entity.Envelope {
	envelope := entity.NewEnvelope(c.gapOpts.Version, entry.Stream, nil).
		WithPayload(body).
		WithGroup(c.group)
	for key, value := range headers {
		envelope.AddHeader(key, value)
	}
	envelope.Log().Debug("redisstreams: received a message",
		slog.String("stream", entry.Stream),
		slog.String("id", entry.ID),
		slog.String("group", c.groupID),
	)
	return envelope
}

func (c *Consumer) deliveryContext(intakeCtx context.Context) context.Context {
	if c.gapOpts != nil {
		return c.gapOpts.WorkContext()
	}
	return intakeCtx
}

func (c *Consumer) ackWithRetry(ctx context.Context, entry streamEntry) error {
	delay := c.commitRetryMin
	for attempt := 1; ; attempt++ {
		err := c.client.Ack(ctx, entry.Stream, c.groupID, entry.ID)
		if err == nil {
			slog.Debug("redisstreams: acknowledged a message",
				slog.String("stream", entry.Stream),
				slog.String("id", entry.ID),
				slog.String("group", c.groupID),
			)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("redisstreams: acknowledge failed; retrying",
			slog.String("stream", entry.Stream),
			slog.String("id", entry.ID),
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

// Commit implements [broker.Consumer].
func (c *Consumer) Commit(tag any) error {
	delivery, err := c.deliveryTag(tag)
	if err != nil {
		return err
	}
	return delivery.submit(deliveryCommit)
}

// Reject implements [broker.Consumer]. Redis has no negative acknowledgement
// that requeues immediately. The entry stays pending and is delivered again
// after a short delay. A crash before XACK leaves it reclaimable.
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

func (c *Consumer) Close() error {
	c.mu.Lock()
	cancel := c.cancel
	c.state = consumerClosed
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}
