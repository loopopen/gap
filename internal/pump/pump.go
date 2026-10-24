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
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/gap/storage"
)

type Sender interface {
	SendAndUpdate(ctx context.Context, envelope *entity.Envelope) error
}

type Handler interface {
	HandleAndUpdate(ctx context.Context, envelope *entity.Envelope) error
}

type Pump struct {
	gapOpts        *ingap.Options
	storage        storage.Storage
	sendCh         chan *entity.Envelope
	handleCh       chan *entity.Envelope
	senderMu       sync.RWMutex
	senders        map[string]Sender
	senderConfigs  map[string]string
	handlers       *sync.Map
	handlerConfigs *sync.Map
	handlerOnce    sync.Once
	dispatchMu     sync.RWMutex
	sealOnce       sync.Once
	sealed         bool
	sourceWG       sync.WaitGroup
	dispatcherWG   sync.WaitGroup
	taskWG         sync.WaitGroup
}

func New(gapOpts *ingap.Options, stor storage.Storage) *Pump {
	if stor == nil {
		panic(errx.ErrNoStorage)
	}

	p := &Pump{
		gapOpts:        gapOpts,
		storage:        stor,
		senders:        make(map[string]Sender),
		senderConfigs:  make(map[string]string),
		handlers:       &sync.Map{},
		handlerConfigs: &sync.Map{},
	}
	p.startSender()
	return p
}

func routeKey(brokerType, brokerName string) string {
	return brokerType + "\x00" + brokerName
}

func (p *Pump) SetSender(brokerType, brokerName, configIdentity string, sender Sender) error {
	p.senderMu.Lock()
	defer p.senderMu.Unlock()
	if p.senders == nil {
		p.senders = make(map[string]Sender)
	}
	if p.senderConfigs == nil {
		p.senderConfigs = make(map[string]string)
	}
	key := routeKey(brokerType, brokerName)
	if existing, ok := p.senderConfigs[key]; ok && existing != configIdentity {
		return fmt.Errorf("broker route %q (%s) has multiple configurations; set a unique BrokerName", brokerName, brokerType)
	}
	if _, ok := p.senders[key]; !ok {
		p.senders[key] = sender
		p.senderConfigs[key] = configIdentity
	}
	return nil
}

func handlerKey(brokerType, brokerName, group string) string {
	return routeKey(brokerType, brokerName) + "\x00" + group
}

func (p *Pump) SetHandler(brokerType, brokerName, configIdentity, group string, handler Handler) error {
	if p.handlers == nil {
		p.handlers = &sync.Map{}
	}
	if p.handlerConfigs == nil {
		p.handlerConfigs = &sync.Map{}
	}
	key := routeKey(brokerType, brokerName)
	if existing, loaded := p.handlerConfigs.LoadOrStore(key, configIdentity); loaded && existing != configIdentity {
		return fmt.Errorf("broker route %q (%s) has multiple configurations; set a unique BrokerName", brokerName, brokerType)
	}
	p.handlers.LoadOrStore(handlerKey(brokerType, brokerName, group), handler)
	return nil
}

func (p *Pump) startSender() {
	o := p.gapOpts
	size := o.PublishBufferSize
	if size < 100 {
		size = 100
	}
	p.sendCh = make(chan *entity.Envelope, size)

	// Polling stops with the intake context; a batch claimed before shutdown is
	// completed on the work context inside pollingSend.
	p.sourceWG.Add(1)
	go func() {
		defer p.sourceWG.Done()
		p.pollingSend(o.Context)
	}()

	// Queued work uses the drain context.
	p.dispatcherWG.Add(1)
	go func() {
		defer p.dispatcherWG.Done()
		p.dispatchingSend(o.DrainContext, o.MaxPublishConcurrency)
	}()
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

	p.sourceWG.Add(1)
	go func() {
		defer p.sourceWG.Done()
		p.pollingHandle(o.Context)
	}()

	p.dispatcherWG.Add(1)
	go func() {
		defer p.dispatcherWG.Done()
		p.dispatchingHandle(o.DrainContext, size)
	}()
}

func (p *Pump) DispatchToSend(ctx context.Context, envelope *entity.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p.dispatchMu.RLock()
	defer p.dispatchMu.RUnlock()
	if p.sealed {
		return ingap.ErrRuntimeStopping
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

	p.dispatchMu.RLock()
	defer p.dispatchMu.RUnlock()
	if p.sealed || p.handleCh == nil {
		return ingap.ErrRuntimeStopping
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
	workCtx := p.gapOpts.DrainContext
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
				err := p.send(workCtx, en)
				if err != nil && ctx.Err() == nil {
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
		p.taskWG.Add(1)
	}

	go func() {
		defer func() {
			<-sem
			p.taskWG.Done()
		}()
		err := p.send(ctx, envelope)
		if err != nil {
			envelope.Log().Warn("failing back to db polling", slog.Any("err", err))
		}
	}()
}

func (p *Pump) sendSerial(ctx context.Context, envelope *entity.Envelope) {
	p.taskWG.Add(1)
	defer p.taskWG.Done()

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
		p.taskWG.Add(1)
	}

	go func() {
		defer func() {
			<-sem
			p.taskWG.Done()
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
	workCtx := p.gapOpts.DrainContext
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
				err := handler.HandleAndUpdate(workCtx, en)
				if err != nil && ctx.Err() == nil {
					wait(ctx, 10*time.Second)
				}
			}
		}
	}
}

func (p *Pump) send(ctx context.Context, envelope *entity.Envelope) error {
	p.senderMu.RLock()
	brokerType := envelope.Headers[entity.BrokerTypeHeader]
	brokerName := envelope.Headers[entity.BrokerNameHeader]
	if sender, ok := p.senders[routeKey(brokerType, brokerName)]; ok {
		p.senderMu.RUnlock()
		return sender.SendAndUpdate(ctx, envelope)
	}
	p.senderMu.RUnlock()
	return p.failPublished(ctx, envelope, fmt.Errorf("sender not found for broker %q (%s)", brokerName, brokerType))
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
	brokerType := envelope.Headers[entity.BrokerTypeHeader]
	brokerName := envelope.Headers[entity.BrokerNameHeader]
	return p.handlers.Load(handlerKey(brokerType, brokerName, envelope.Group))
}

func wait(ctx context.Context, duration time.Duration) {
	select {
	case <-time.After(duration):
	case <-ctx.Done():
	}
}

func (p *Pump) StopSources(ctx context.Context) error {
	return waitGroup(ctx, &p.sourceWG)
}

// Seal rejects future dispatches and closes dispatcher inputs. Callers must
// first stop all known producers of work; concurrent late dispatches are still
// safe because dispatchMu prevents sends racing with channel closure.
func (p *Pump) Seal() {
	p.sealOnce.Do(func() {
		p.dispatchMu.Lock()
		p.sealed = true
		close(p.sendCh)
		if p.handleCh != nil {
			close(p.handleCh)
		}
		p.dispatchMu.Unlock()
	})
}

// Drain waits first for dispatchers to consume every queued item. Only after
// dispatchers stop can taskWG be known to receive no future Add calls.
func (p *Pump) Drain(ctx context.Context) error {
	if err := waitGroup(ctx, &p.dispatcherWG); err != nil {
		return err
	}
	if err := waitGroup(ctx, &p.taskWG); err != nil {
		return err
	}
	slog.Info("gap drained successfully")
	return nil
}

func waitGroup(ctx context.Context, wg *sync.WaitGroup) error {
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

func (p *Pump) StartHandler() {
	p.startHandler()
}
