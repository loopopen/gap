package pump

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/internal/errx"
	"github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/internal/plugin"
	"github.com/loopopen/gap/storage"
)

var pump *Pump
var newOnce sync.Once

type Sender interface {
	SendAndUpdate(ctx context.Context, envelope *entity.Envelope) error
}

type Handler interface {
	HandleAndUpdate(ctx context.Context, envelope *entity.Envelope) error
}

type Pump struct {
	gapOpts        *gap.Options
	storage        storage.Storage
	sendCh         chan *entity.Envelope
	handleCh       chan *entity.Envelope
	senderMu       sync.RWMutex
	senders        map[string]Sender
	senderConfigs  map[string]string
	handlers       *sync.Map
	handlerConfigs *sync.Map
	handlerOnce    sync.Once
	wg             *sync.WaitGroup
}

func Singleton(gapOpts *gap.Options) *Pump {
	stor := plugin.MustGetStorage(gapOpts)
	if stor == nil {
		panic(errx.ErrNoStorage)
	}

	newOnce.Do(func() {
		pump = &Pump{
			gapOpts:        gapOpts,
			storage:        stor,
			senders:        make(map[string]Sender),
			senderConfigs:  make(map[string]string),
			handlers:       &sync.Map{},
			handlerConfigs: &sync.Map{},
			wg:             &sync.WaitGroup{},
		}

		pump.startSender()
	})

	return pump
}

func routeKey(pluginType, brokerName string) string {
	return pluginType + "\x00" + brokerName
}

func (p *Pump) SetSender(pluginType, brokerName, configIdentity string, sender Sender) error {
	p.senderMu.Lock()
	defer p.senderMu.Unlock()
	if p.senders == nil {
		p.senders = make(map[string]Sender)
	}
	if p.senderConfigs == nil {
		p.senderConfigs = make(map[string]string)
	}
	key := routeKey(pluginType, brokerName)
	if existing, ok := p.senderConfigs[key]; ok && existing != configIdentity {
		return fmt.Errorf("broker route %q (%s) has multiple configurations; set a unique BrokerName", brokerName, pluginType)
	}
	if _, ok := p.senders[key]; !ok {
		p.senders[key] = sender
		p.senderConfigs[key] = configIdentity
	}
	return nil
}

func handlerKey(pluginType, brokerName, group string) string {
	return routeKey(pluginType, brokerName) + "\x00" + group
}

func (p *Pump) SetHandler(pluginType, brokerName, configIdentity, group string, handler Handler) error {
	if p.handlers == nil {
		p.handlers = &sync.Map{}
	}
	if p.handlerConfigs == nil {
		p.handlerConfigs = &sync.Map{}
	}
	key := routeKey(pluginType, brokerName)
	if existing, loaded := p.handlerConfigs.LoadOrStore(key, configIdentity); loaded && existing != configIdentity {
		return fmt.Errorf("broker route %q (%s) has multiple configurations; set a unique BrokerName", brokerName, pluginType)
	}
	p.handlers.LoadOrStore(handlerKey(pluginType, brokerName, group), handler)
	return nil
}

func (p *Pump) startSender() {
	o := p.gapOpts
	size := o.PublishBufferSize
	if size < 100 {
		size = 100
	}
	p.sendCh = make(chan *entity.Envelope, size)

	//for simplicity, the compensation logic uses the shutdown context
	go p.pollingSend(o.Context)

	//the main logic uses the drain context
	go p.dispatchingSend(o.DrainContext, o.MaxPublishConcurrency)
}

func (p *Pump) startHandler() {
	p.handlerOnce.Do(p.startHandlerOnce)
}

func (p *Pump) startHandlerOnce() {
	o := p.gapOpts
	size := runtime.GOMAXPROCS(0) * o.WorkConcurrencyFactor
	if size < 1 {
		size = 1
	}
	p.handleCh = make(chan *entity.Envelope, size)

	go p.pollingHandle(o.Context)

	go p.dispatchingHandle(o.DrainContext, size)
}

func (p *Pump) DispatchToSend(ctx context.Context, envelope *entity.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case p.sendCh <- envelope:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return errors.New("system backpressure: send buffer overflow")
	}
}

func (p *Pump) DispatchToHandle(ctx context.Context, envelope *entity.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case p.handleCh <- envelope:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return errors.New("system backpressure: handle buffer overflow")
	}
}

func (p *Pump) pollingSend(ctx context.Context) {
	ticker := time.NewTicker(p.gapOpts.PumpInterval())
	defer ticker.Stop()

	defer func() {
		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				slog.Info("gap pump stopped sending")
			} else {
				slog.Error("gap pump stopped sending with error", slog.Any("err", ctx.Err()))
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if !p.hasSender() {
				continue
			}

			ps, err := p.storage.ClaimPublishedBatch(ctx, p.gapOpts.ClaimBatchSize)
			if err != nil {
				slog.Error("failed to claim published messages", slog.Any("err", err))
				wait(ctx, 10*time.Second)
				continue
			}

			for _, en := range ps {
				err := p.send(ctx, en)
				if err != nil {
					wait(ctx, 10*time.Second)
				}
			}
		}
	}
}

func (p *Pump) dispatchingSend(ctx context.Context, concurrency int) {
	if concurrency < 1 {
		for {
			select {
			case <-ctx.Done():
				return

			case en, ok := <-p.sendCh:
				if !ok {
					return
				}
				p.sendSerial(ctx, en)
			}
		}
		//return
	}

	sem := make(chan struct{}, concurrency)
	for {
		select {
		case <-ctx.Done():
			return

		case en, ok := <-p.sendCh:
			if !ok {
				return
			}
			p.sendParallel(ctx, en, sem)
		}
	}
}

func (p *Pump) sendParallel(ctx context.Context, envelope *entity.Envelope, sem chan struct{}) {
	select {
	case <-ctx.Done():
		return
	case sem <- struct{}{}:
		p.wg.Add(1)
	}

	go func() {
		defer func() {
			<-sem
			p.wg.Done()
		}()
		err := p.send(ctx, envelope)
		if err != nil {
			envelope.Log().Warn("failing back to db polling", slog.Any("err", err))
		}
	}()
}

func (p *Pump) sendSerial(ctx context.Context, envelope *entity.Envelope) {
	p.wg.Add(1)
	defer p.wg.Done()

	err := p.send(ctx, envelope)
	if err != nil {
		envelope.Log().Warn("failing back to db polling", slog.Any("err", err))
	}
}

func (p *Pump) dispatchingHandle(ctx context.Context, concurrency int) {
	if concurrency < 1 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)

	for {
		select {
		case <-ctx.Done():
			return

		case en, ok := <-p.handleCh:
			if !ok {
				return
			}
			p.handleParallel(ctx, en, sem)
		}
	}
}

func (p *Pump) handleParallel(ctx context.Context, envelope *entity.Envelope, sem chan struct{}) {
	select {
	case <-ctx.Done():
		return
	case sem <- struct{}{}:
		p.wg.Add(1)
	}

	go func() {
		defer func() {
			<-sem
			p.wg.Done()
		}()

		h, ok := p.handler(envelope)
		if !ok {
			envelope.Log().Error("no handler found in pump", slog.String("group", envelope.Group))
			return
		}
		handler := h.(Handler)
		err := handler.HandleAndUpdate(ctx, envelope)
		if err != nil {
			envelope.Log().Warn("falling back to db polling", slog.Any("err", err))
		}
	}()
}

func (p *Pump) pollingHandle(ctx context.Context) {
	ticker := time.NewTicker(p.gapOpts.PumpInterval())
	defer ticker.Stop()

	defer func() {
		if errors.Is(ctx.Err(), context.Canceled) {
			slog.Info("gap pump stopped handling")
		} else {
			slog.Error("gap pump stopped handling with error", slog.Any("err", ctx.Err()))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			rs, err := p.storage.ClaimReceivedBatch(ctx, p.gapOpts.ClaimBatchSize)
			if err != nil {
				slog.Error("failed to claim received messages", slog.Any("err", err))
				wait(ctx, 10*time.Second)
				continue
			}

			for _, en := range rs {
				h, ok := p.handler(en)
				if !ok {
					en.Log().Error("no handler found in pump", slog.String("group", en.Group))
					continue
				}
				handler := h.(Handler)
				err := handler.HandleAndUpdate(ctx, en)
				if err != nil {
					wait(ctx, 10*time.Second)
				}
			}
		}
	}
}

func (p *Pump) send(ctx context.Context, envelope *entity.Envelope) error {
	p.senderMu.RLock()
	pluginType := envelope.Headers[entity.PluginTypeHeader]
	brokerName := envelope.Headers[entity.BrokerNameHeader]
	if sender, ok := p.senders[routeKey(pluginType, brokerName)]; ok {
		p.senderMu.RUnlock()
		return sender.SendAndUpdate(ctx, envelope)
	}
	p.senderMu.RUnlock()
	return p.failPublished(ctx, envelope, fmt.Errorf("sender not found for broker %q (%s)", brokerName, pluginType))
}

func (p *Pump) failPublished(ctx context.Context, envelope *entity.Envelope, cause error) error {
	if p.storage == nil {
		return cause
	}
	if err := p.storage.UpdateStatusPublished(ctx, envelope.ID, 0, enum.StatusFailed); err != nil {
		return fmt.Errorf("%w; mark published message failed: %v", cause, err)
	}
	return cause
}

func (p *Pump) hasSender() bool {
	p.senderMu.RLock()
	defer p.senderMu.RUnlock()
	return len(p.senders) != 0
}

func (p *Pump) handler(envelope *entity.Envelope) (any, bool) {
	pluginType := envelope.Headers[entity.PluginTypeHeader]
	brokerName := envelope.Headers[entity.BrokerNameHeader]
	return p.handlers.Load(handlerKey(pluginType, brokerName, envelope.Group))
}

func wait(ctx context.Context, duration time.Duration) {
	select {
	case <-time.After(duration):
	case <-ctx.Done():
	}
}

func (p *Pump) AddOne() {
	p.wg.Add(1)
}

func (p *Pump) Done() {
	p.wg.Done()
}

func WaitDrain(ctx context.Context) error {
	if pump == nil {
		return nil
	}

	done := make(chan struct{})
	go func() {
		pump.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		slog.Info("gap drained successfully")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func StartHandler() {
	if pump != nil {
		pump.startHandler()
	}
}
