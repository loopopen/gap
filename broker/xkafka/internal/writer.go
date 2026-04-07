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
	"github.com/lopolopen/gap/internal/gap"
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
	opts := gapOpts.BrokerOptions.(*Options)

	writer := &Writer{
		gapOpts:     gapOpts,
		opts:        opts,
		connFactory: NewConnFactory(opts),
		ensurer:     SingleEnsurer(opts),
	}

	var _ broker.Writer = writer
	return writer
}

func (w *Writer) init() error {
	_, err := w.connFactory.CreateConn(false)
	if err != nil {
		return err
	}

	w.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:      w.opts.Brokers,
		Dialer:       w.connFactory.CreaterDialer(),
		Balancer:     &kafka.LeastBytes{},
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		BatchTimeout: 100 * time.Millisecond,
	})
	return nil
}

// Write implements [gap.Writer].
func (w *Writer) Write(ctx context.Context, envelope *entity.Envelope) error {
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

	if err := w.ensurer.ensureTopic(ctx, envelope.Topic); err != nil {
		return err
	}

	slog.Debug("kafka: send the message",
		slog.String("topic", envelope.Topic),
		slog.String("id", envelope.IDString()),
	)
	return w.send(ctx, envelope.Topic, envelope.Headers, body)
}

func (w *Writer) send(ctx context.Context, topic string, headers map[string]string, body []byte) error {
	hds := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		hds = append(hds, kafka.Header{Key: k, Value: []byte(v)})
	}

	msg := kafka.Message{
		Topic:   topic,
		Value:   body,
		Headers: hds,
	}

	return w.writer.WriteMessages(ctx, msg)
}
