package internal

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/loopopen/gap/v2/broker"
	"github.com/loopopen/gap/v2/spi/entity"
	"github.com/loopopen/gap/v2/spi/errx"
	internalgap "github.com/loopopen/gap/v2/spi/runtime"
)

type producerClient interface {
	Produce(message *kafka.Message, deliveryChan chan kafka.Event) error
}

type Producer struct {
	gapOpts  *internalgap.Options
	opts     *Options
	producer producerClient
	ensurer  *Ensurer
}

func NewProducer(gapOpts *internalgap.Options, opts *Options) (*Producer, error) {
	ensurer, err := SingleEnsurer(opts)
	if err != nil {
		return nil, err
	}
	return &Producer{gapOpts: gapOpts, opts: opts, ensurer: ensurer}, nil
}

func (p *Producer) init() error {
	if err := p.ensurer.ensure(p.gapOpts.Context); err != nil {
		return err
	}
	config := clientConfig(p.opts)
	producer, err := kafka.NewProducer(&config)
	if err != nil {
		return err
	}
	p.producer = producer
	return nil
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

	headers := make([]kafka.Header, 0, len(envelope.Headers))
	for key, value := range envelope.Headers {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	topic := envelope.Topic
	delivery := make(chan kafka.Event, 1)
	if err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          body,
		Headers:        headers,
	}, delivery); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case event := <-delivery:
		message, ok := event.(*kafka.Message)
		if !ok {
			return fmt.Errorf("confluent: unexpected delivery event %T", event)
		}
		if message.TopicPartition.Error != nil {
			return message.TopicPartition.Error
		}
		envelope.Log().Debug("confluent: produced a message")
		return nil
	}
}

var _ broker.Producer = (*Producer)(nil)
