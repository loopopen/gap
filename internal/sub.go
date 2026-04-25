package internal

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/lopolopen/gap/broker"
	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/gap/internal/errx"
	"github.com/lopolopen/gap/internal/gap"
	"github.com/lopolopen/gap/internal/plugin"
	"github.com/lopolopen/gap/internal/pump"
	"github.com/lopolopen/gap/storage"
)

var groupedSubs *GroupedSubs
var newOnce sync.Once
var fixOnce sync.Once

type GroupedSubs struct {
	subs              map[string]*Sub
	HandlerOtpsLst    []gap.HandlerOptions
	DependencyOtpsLst []gap.DependencyOptions
}

func SingleGroupedSubs() *GroupedSubs {
	newOnce.Do(func() {
		groupedSubs = &GroupedSubs{
			subs: make(map[string]*Sub),
		}
	})
	return groupedSubs
}

func (g *GroupedSubs) AddDependencyOtps(lst []gap.DependencyOptions) {
	g.DependencyOtpsLst = append(g.DependencyOtpsLst, lst...)
}

func (g *GroupedSubs) AddHandlerOtps(lst []gap.HandlerOptions) {
	g.HandlerOtpsLst = append(g.HandlerOtpsLst, lst...)
}

func (g *GroupedSubs) SubscribeAll(gapOpts *gap.Options) error {
	//todo: IngestConcurrency
	for _, o := range g.HandlerOtpsLst {
		if o.Handler == nil {
			return errx.ErrNilHandler
		}
		if o.Topic == "" {
			return errx.ErrEmptyTopic
		}
		err := g.subscribeOne(gapOpts, &o)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *GroupedSubs) subscribeOne(gapOpts *gap.Options, opts *gap.HandlerOptions) error {
	group := opts.Group

	sub, ok := g.subs[group]
	if !ok {
		reader := plugin.MustGetRBroker(gapOpts, group)
		if reader == nil {
			panic("reader broker must not be nil")
		}
		stor := plugin.MustGetStorage(gapOpts)
		if stor != nil {
			fixOnce.Do(func() {
				err := stor.UpdateStatusReceived(gapOpts.Context, 0, enum.StatusProcessing, enum.StatusFailed)
				if err != nil {
					panic(err)
				}
			})
		}

		sub = NewSub(gapOpts, &opts.GroupOptions, reader, stor)
		g.subs[group] = sub
	}
	err := sub.Subscribe(opts.Topic, opts.Handler)
	if err != nil {
		return err
	}
	return nil
}

func (g *GroupedSubs) ListeningAll() error {
	for _, sub := range g.subs {
		err := sub.Listening()
		if err != nil {
			return err
		}
	}
	return nil
}

type Sub struct {
	opts      *gap.Options
	groupOpts *gap.GroupOptions
	storage   storage.Storage
	reader    broker.Reader
	handlers  map[string]gap.Handler[[]byte]
	pump      *pump.Pump
}

// Subscribe implements [Subscriber].
func (s *Sub) Subscribe(topic string, handler gap.Handler[[]byte]) error {
	_, ok := s.handlers[topic]
	if ok {
		return errx.ErrMultiHandlers(topic, s.groupOpts.Group)
	}
	s.handlers[topic] = handler

	slog.Debug(fmt.Sprintf("subscribe: topic(%s) -> group(%s:%d)", topic, s.groupOpts.Group, s.groupOpts.IngestConcurrency))

	ctx := s.opts.Context
	err := s.reader.Subscribe(ctx, topic)
	if err != nil {
		return err
	}

	return nil
}

func NewSub(opts *gap.Options, groupOpts *gap.GroupOptions, reader broker.Reader, storage storage.Storage) *Sub {
	if reader == nil {
		panic(errx.ErrNoBroker)
	}

	sub := &Sub{
		opts:      opts,
		groupOpts: groupOpts,
		storage:   storage,
		reader:    reader,
		handlers:  make(map[string]gap.Handler[[]byte]),
	}

	if storage != nil {
		pump := pump.Singleton(opts)
		pump.SetHandler(groupOpts.Group, sub)
		sub.pump = pump
	} else {
		slog.Debug("sub works on no-persistence mode")
	}
	return sub
}

func (s *Sub) Listening() error {
	ctx := s.opts.DrainContext
	enCh, err := s.reader.Read(ctx)
	if err != nil {
		return err
	}

	if s.storage != nil {
		concurrency := s.groupOpts.IngestConcurrency
		go s.listening(ctx, enCh, concurrency)
		return nil
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case en, ok := <-enCh:
				if !ok {
					return
				}

				slog.Warn("hande message without persistence")
				err := s.handle(ctx, en)
				if err != nil {
					s.reader.Reject(en.Tag)
				} else {
					s.reader.Commit(en.Tag)
				}
			}
		}
	}()
	return nil
}

func (s *Sub) listening(ctx context.Context, enCh <-chan *entity.Envelope, concurrency int) {
	if concurrency < 1 {
		for {
			select {
			case <-ctx.Done():
				return

			case en, ok := <-enCh:
				if !ok {
					return
				}
				s.ingestSerial(ctx, en)
			}
		}
		//return
	}

	sem := make(chan struct{}, concurrency)
	for {
		select {
		case <-ctx.Done():
			return

		case en, ok := <-enCh:
			if !ok {
				return
			}
			s.ingestParallel(ctx, en, sem)
		}
	}
}

func (s *Sub) ingestParallel(ctx context.Context, envelope *entity.Envelope, sem chan struct{}) {
	select {
	case <-ctx.Done():
		return
	case sem <- struct{}{}:
	}

	go func() {
		defer func() { <-sem }()
		s.ingestSerial(ctx, envelope)
	}()
}

func (s *Sub) ingestSerial(ctx context.Context, envelope *entity.Envelope) {
	s.pump.AddOne()
	defer s.pump.Done()

	err := s.storage.CreateReceived(ctx, envelope)
	if err != nil {
		envelope.Log().Error("failed to create received record", slog.Any("err", err))
		if err := s.reader.Reject(envelope.Tag); err != nil {
			envelope.Log().Error("failed to reject message", slog.Any("err", err))
		}
		return
	}
	if err := s.reader.Commit(envelope.Tag); err != nil {
		envelope.Log().Error("failed to commit message", slog.Any("err", err))
		return
	}

	s.dispatch(ctx, envelope)
}

func (s *Sub) handle(ctx context.Context, envelope *entity.Envelope) (err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			if err, ok = r.(error); !ok {
				envelope.Log().Error("recovered from panic when handling", slog.Any("err", r))
				err = fmt.Errorf("%v", r)
			}
		}
	}()

	handler, ok := s.handlers[envelope.Topic]
	if !ok {
		return errx.ErrHandlerNotFound(envelope.Topic, s.groupOpts.Group)
	}
	payload, err := envelope.PayloadBytes()
	if err != nil {
		return err
	}
	err = handler(ctx, payload, envelope.Headers)
	if err != nil {
		return err
	}
	return nil
}

func (s *Sub) HandleAndUpdate(ctx context.Context, envelope *entity.Envelope) error {
	err := s.handle(ctx, envelope)
	if err != nil {
		envelope.Log().Error("failed to handle message", slog.Any("err", err))

		if err := s.storage.UpdateStatusReceived(ctx, envelope.ID, 0, enum.StatusFailed); err != nil {
			envelope.Log().Error("failed to set received status to Failed", slog.Any("err", err))
			return err
		}
		return err
	}

	if err := s.storage.UpdateStatusReceived(ctx, envelope.ID, 0, enum.StatusSucceeded); err != nil {
		envelope.Log().Warn("failed to set received status to Succeeded", slog.Any("err", err))
		return err
	}
	return nil
}

func (s *Sub) dispatch(ctx context.Context, envelope *entity.Envelope) {
	err := s.pump.DispatchToHandle(ctx, envelope)
	if err != nil {
		envelope.Log().Warn("failed to dispatch envelope to handler, falling back to db polling", slog.Any("err", err))
	}
}
