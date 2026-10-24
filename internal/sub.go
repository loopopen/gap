package internal

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/errx"
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/plugin"
	"github.com/loopopen/gap/internal/pump"
	"github.com/loopopen/gap/storage"
)

type GroupedSubs struct {
	subs              map[string]*Sub
	HandlerOtpsLst    []ingap.HandlerOptions
	DependencyOtpsLst []ingap.DependencyOptions
	fixOnce           sync.Once
}

func NewGroupedSubs() *GroupedSubs {
	return &GroupedSubs{subs: make(map[string]*Sub)}
}

func (g *GroupedSubs) AddDependencyOtps(lst []ingap.DependencyOptions) {
	g.DependencyOtpsLst = append(g.DependencyOtpsLst, lst...)
}

func (g *GroupedSubs) AddHandlerOtps(lst []ingap.HandlerOptions) {
	g.HandlerOtpsLst = append(g.HandlerOtpsLst, lst...)
}

func (g *GroupedSubs) SubscribeAll(gapOpts *ingap.Options, brokerOpts *ingap.BrokerOptions, stor storage.Storage, messagePump *pump.Pump) error {
	//todo: IngestConcurrency
	for _, o := range g.HandlerOtpsLst {
		if o.Handler == nil {
			return errx.ErrNilHandler
		}
		if o.Topic == "" {
			return errx.ErrEmptyTopic
		}
		err := g.subscribeOne(gapOpts, brokerOpts, stor, messagePump, &o)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *GroupedSubs) subscribeOne(gapOpts *ingap.Options, brokerOpts *ingap.BrokerOptions, stor storage.Storage, messagePump *pump.Pump, opts *ingap.HandlerOptions) error {
	group := opts.Group
	brokerName := plugin.BrokerName(brokerOpts)
	brokerType := plugin.BrokerType(brokerOpts)
	configIdentity := plugin.BrokerConfigIdentity(brokerOpts)
	key := brokerType + "\x00" + brokerName + "\x00" + group

	sub, ok := g.subs[key]
	if !ok {
		consumer := plugin.MustGetConsumer(gapOpts, brokerOpts, group)
		if consumer == nil {
			panic("consumer broker must not be nil")
		}
		if stor != nil {
			g.fixOnce.Do(func() {
				err := stor.UpdateStatusReceived(gapOpts.Context, 0, enum.StatusProcessing, enum.StatusFailed)
				if err != nil {
					panic(err)
				}
			})
		}

		sub = NewSub(gapOpts, &opts.GroupOptions, consumer, stor, messagePump, brokerType, brokerName, configIdentity)
		g.subs[key] = sub
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

func (g *GroupedSubs) Stop(ctx context.Context) error {
	for _, sub := range g.subs {
		if err := sub.Stop(ctx); err != nil {
			return err
		}
	}
	return nil
}

type Sub struct {
	opts       *ingap.Options
	groupOpts  *ingap.GroupOptions
	storage    storage.Storage
	consumer   broker.Consumer
	brokerName string
	brokerType string
	handlers   map[string]ingap.Handler[[]byte]
	pump       *pump.Pump
	listenOnce sync.Once
	listenErr  error
	listenWG   sync.WaitGroup
	ingestWG   sync.WaitGroup
}

// Subscribe implements [Subscriber].
func (s *Sub) Subscribe(topic string, handler ingap.Handler[[]byte]) error {
	_, ok := s.handlers[topic]
	if ok {
		return errx.ErrMultiHandlers(topic, s.groupOpts.Group)
	}
	s.handlers[topic] = handler

	slog.Debug(fmt.Sprintf("subscribe: topic(%s) -> group(%s:%d)", topic, s.groupOpts.Group, s.groupOpts.IngestConcurrency))

	ctx := s.opts.Context
	err := s.consumer.Subscribe(ctx, topic)
	if err != nil {
		return err
	}

	return nil
}

func NewSub(opts *ingap.Options, groupOpts *ingap.GroupOptions, consumer broker.Consumer, storage storage.Storage, messagePump *pump.Pump, brokerType, brokerName, configIdentity string) *Sub {
	if consumer == nil {
		panic(errx.ErrNoBroker)
	}

	sub := &Sub{
		opts:       opts,
		groupOpts:  groupOpts,
		storage:    storage,
		consumer:   consumer,
		brokerName: brokerName,
		brokerType: brokerType,
		handlers:   make(map[string]ingap.Handler[[]byte]),
	}

	if storage != nil {
		if messagePump == nil {
			panic("message pump must not be nil when storage is configured")
		}
		if err := messagePump.SetHandler(brokerType, brokerName, configIdentity, groupOpts.Group, sub); err != nil {
			panic(err)
		}
		sub.pump = messagePump
	} else {
		slog.Debug("sub works on no-persistence mode")
	}
	return sub
}

func (s *Sub) Listening() error {
	s.listenOnce.Do(func() {
		s.listenErr = s.startListening()
	})
	return s.listenErr
}

func (s *Sub) startListening() error {
	intakeCtx := s.opts.Context
	workCtx := s.opts.DrainContext
	enCh, err := s.consumer.Consume(intakeCtx)
	if err != nil {
		return err
	}

	if s.storage != nil {
		concurrency := s.groupOpts.IngestConcurrency
		s.listenWG.Add(1)
		go func() {
			defer s.listenWG.Done()
			s.listening(intakeCtx, workCtx, enCh, concurrency)
		}()
		return nil
	}

	s.listenWG.Add(1)
	go func() {
		defer s.listenWG.Done()
		for {
			select {
			case <-intakeCtx.Done():
				return
			case en, ok := <-enCh:
				if !ok {
					return
				}
				if intakeCtx.Err() != nil {
					_ = s.consumer.Reject(en.Tag)
					return
				}

				slog.Warn("handle message without persistence")
				err := s.handle(workCtx, en)
				if err != nil {
					s.consumer.Reject(en.Tag)
				} else {
					s.consumer.Commit(en.Tag)
				}
			}
		}
	}()
	return nil
}

func (s *Sub) listening(intakeCtx, workCtx context.Context, enCh <-chan *entity.Envelope, concurrency int) {
	if concurrency < 1 {
		for {
			select {
			case <-intakeCtx.Done():
				return

			case en, ok := <-enCh:
				if !ok {
					return
				}
				if intakeCtx.Err() != nil {
					_ = s.consumer.Reject(en.Tag)
					return
				}
				s.ingestSerial(workCtx, en)
			}
		}
		//return
	}

	sem := make(chan struct{}, concurrency)
	for {
		select {
		case <-intakeCtx.Done():
			return

		case en, ok := <-enCh:
			if !ok {
				return
			}
			if intakeCtx.Err() != nil {
				_ = s.consumer.Reject(en.Tag)
				return
			}
			s.ingestParallel(workCtx, en, sem)
		}
	}
}

func (s *Sub) ingestParallel(ctx context.Context, envelope *entity.Envelope, sem chan struct{}) {
	select {
	case <-ctx.Done():
		return
	case sem <- struct{}{}:
	}

	s.ingestWG.Add(1)
	go func() {
		defer func() {
			<-sem
			s.ingestWG.Done()
		}()
		s.ingestSerial(ctx, envelope)
	}()
}

func (s *Sub) ingestSerial(ctx context.Context, envelope *entity.Envelope) {
	envelope.AddHeader(KeysBrokerName, s.brokerName)
	envelope.AddHeader(KeysBrokerType, s.brokerType)
	err := s.storage.CreateReceived(ctx, envelope)
	if err != nil {
		envelope.Log().Error("failed to create received record", slog.Any("err", err))
		if err := s.consumer.Reject(envelope.Tag); err != nil {
			envelope.Log().Error("failed to reject message", slog.Any("err", err))
		}
		return
	}
	if err := s.consumer.Commit(envelope.Tag); err != nil {
		envelope.Log().Error("failed to commit message", slog.Any("err", err))
		return
	}

	s.dispatch(ctx, envelope)
}

func (s *Sub) Stop(ctx context.Context) error {
	if err := waitGroupContext(ctx, &s.listenWG); err != nil {
		return err
	}
	return waitGroupContext(ctx, &s.ingestWG)
}

func waitGroupContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
		envelope.Log().Error("failed to handle received message", slog.Any("err", err))

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
