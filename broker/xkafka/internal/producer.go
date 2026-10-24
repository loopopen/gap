package internal

import (
	"context"
	"sync"
	"time"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/segmentio/kafka-go"
)

type Producer struct {
	gapOpts   *ingap.Options
	opts      *Options
	client    *kafka.Client
	transport *kafka.Transport
	group     string
	writer    *kafka.Writer
	groupID   string
	topics    []string
	topicMu   sync.Mutex
	ensurer   *Ensurer
}

// NewProducer creates a new Kafka producer broker instance.
func NewProducer(gapOpts *ingap.Options, opts *Options) *Producer {
	producer := &Producer{
		gapOpts:   gapOpts,
		opts:      opts,
		client:    SingleClient(opts),
		transport: SingleTransport(opts),
		ensurer:   SingleEnsurer(opts),
	}

	var _ broker.Producer = producer
	return producer
}

func (p *Producer) init() error {
	err := p.ensurer.ensure(p.gapOpts.Context)
	if err != nil {
		return err
	}

	p.writer = newKafkaWriter(p.opts, p.transport)
	return nil
}

func newKafkaWriter(opts *Options, transport *kafka.Transport) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(opts.Brokers...),
		Balancer:     &kafka.LeastBytes{},
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		BatchTimeout: 100 * time.Millisecond,
		Transport:    transport,
	}
}

// Produce implements [broker.Producer].
func (p *Producer) Produce(ctx context.Context, envelope *entity.Envelope) error {
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

	if err := p.ensurer.ensureTopic(ctx, envelope.Topic); err != nil {
		return err
	}

	envelope.Log().Debug("kafka: send the message")
	return p.send(ctx, envelope.Topic, envelope.Headers, body)
}

func (p *Producer) send(ctx context.Context, topic string, headers map[string]string, body []byte) error {
	hds := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		hds = append(hds, kafka.Header{Key: k, Value: []byte(v)})
	}

	msg := kafka.Message{
		Topic:   topic,
		Value:   body,
		Headers: hds,
	}

	return p.writer.WriteMessages(ctx, msg)
}
