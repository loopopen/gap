package internal

import (
	"context"
	"log/slog"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
)

type Producer struct {
	gapOpts *ingap.Options
	opts    *Options
	client  streamClient
}

func NewProducer(gapOpts *ingap.Options, opts *Options) (*Producer, error) {
	client, err := SingleClient(opts)
	if err != nil {
		return nil, err
	}
	producer := &Producer{
		gapOpts: gapOpts,
		opts:    opts,
		client:  client,
	}
	var _ broker.Producer = producer
	return producer, nil
}

func (p *Producer) init() error {
	return p.client.Ping(p.gapOpts.IntakeContext())
}

// Produce implements [broker.Producer].
func (p *Producer) Produce(ctx context.Context, envelope *entity.Envelope) error {
	if envelope == nil {
		return errx.Permanent(errx.ErrParamIsNil("envelope"))
	}
	if envelope.Topic == "" {
		return errx.Permanent(errx.ErrEmptyTopic)
	}
	body, err := envelope.PayloadBytes()
	if err != nil {
		return errx.Permanent(err)
	}
	if len(body) == 0 {
		return errx.Permanent(errx.ErrNilPayload)
	}
	values, err := streamValues(envelope.Headers, body)
	if err != nil {
		return errx.Permanent(err)
	}

	envelope.Log().Debug("redisstreams: send the message")
	id, err := p.client.Add(ctx, envelope.Topic, p.opts.MaxLen, values)
	if err != nil {
		return err
	}
	envelope.Log().Debug("redisstreams: published a message", slog.String("id", id))
	return nil
}

// Close implements [broker.Closer]. The Redis client is shared by every runtime
// using the same configuration and is left open.
func (p *Producer) Close() error {
	return nil
}

var _ broker.Closer = (*Producer)(nil)
