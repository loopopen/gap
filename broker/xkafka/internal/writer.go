package internal

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/internal"
	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/errx"
	"github.com/lopolopen/gap/options/gap"
	"github.com/segmentio/kafka-go"
)

const (
	// Topic creation default timeouts
	topicCreateTimeout = 30 * time.Second
	topicCreateRetries = 5

	// Topic validation retry backoff
	topicValidateDelay = 500 * time.Millisecond

	corrid = internal.KeysCorrelationID
)

type Writer struct {
	gapOpts     *gap.Options
	opts        *Options
	group       string
	connFactory *ConnFactory
	writer      *kafka.Writer
	groupID     string
	topics      []string
	topicMu     sync.Mutex
	ensurer     *Ensurer
}

// NewWriter creates a new Kafka writer broker instance.
func NewWriter(gapOpts *gap.Options) *Writer {
	bp := gapOpts.BrokerPlugin
	opts := bp.(*Options)

	writer := &Writer{
		gapOpts:     gapOpts,
		opts:        opts,
		connFactory: NewConnFactory(opts),
		ensurer:     SingleEnsurer(opts),
	}

	var _ broker.Writer = writer
	return writer
}

func (b *Writer) init() error {
	_, err := b.connFactory.CreateConn(false)
	if err != nil {
		return err
	}

	b.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:      b.opts.Brokers,
		Dialer:       b.connFactory.CreaterDialer(),
		Balancer:     &kafka.LeastBytes{},
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		BatchTimeout: 100 * time.Millisecond,
	})
	return nil
}

// Write implements [gap.Writer].
func (b *Writer) Write(ctx context.Context, envelope *entity.Envelope) error {
	if envelope == nil {
		return errx.ErrParamIsNil("envelope")
	}
	if envelope.Topic == "" {
		return errx.ErrEmptyTopic
	}
	body, err := envelope.PayloadBytes()
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return errx.ErrNilPayload
	}

	if err := b.ensurer.ensureTopic(ctx, envelope.Topic); err != nil {
		return err
	}

	slog.Debug("kafka: send the message",
		slog.String("topic", envelope.Topic),
		slog.String("id", envelope.IDString()),
	)
	return b.send(ctx, envelope.Topic, envelope.Headers, body)
}

func (b *Writer) send(ctx context.Context, topic string, headers map[string]string, body []byte) error {
	hds := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		hds = append(hds, kafka.Header{Key: k, Value: []byte(v)})
	}

	msg := kafka.Message{
		Topic:   topic,
		Value:   body,
		Headers: hds,
	}

	return b.writer.WriteMessages(ctx, msg)
}
