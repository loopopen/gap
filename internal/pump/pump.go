package pump

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/errx"
	"github.com/lopolopen/gap/internal/registry"
	"github.com/lopolopen/gap/options/gap"
	"github.com/lopolopen/gap/storage"
)

var pump *Pump
var once sync.Once

type Pump struct {
	gapOpts  *gap.Options
	storage  storage.Storage
	sendCh   chan *entity.Envelope
	handleCh chan *entity.Envelope
	sender   Sender
	handlers *sync.Map
	wg       *sync.WaitGroup
}

func Singleton(gapOpts *gap.Options) *Pump {
	stor := registry.MustGetStorage(gapOpts)
	if stor == nil {
		panic(errx.ErrNoStorage)
	}

	once.Do(func() {
		pump = &Pump{
			gapOpts:  gapOpts,
			storage:  stor,
			handlers: &sync.Map{},
			wg:       &sync.WaitGroup{},
		}

		pump.startSender()
	})

	return pump
}

func (p *Pump) SetSender(sender Sender) {
	if p.sender != nil {
		return
	}
	p.sender = sender
}

func (p *Pump) SetHandler(group string, handler Handler) {
	_, ok := p.handlers.Load(group)
	if ok {
		return
	}
	p.handlers.Store(group, handler)
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
			if p.sender == nil {
				continue
			}

			es, err := p.storage.ClaimPublishedBatch(ctx, p.gapOpts.ClaimBatchSize)
			if err != nil {
				slog.Error("failed to claim published messages", slog.Any("err", err))
				wait(ctx, 10*time.Second)
				continue
			}

			for _, e := range es {
				err := p.sender.SendAndUpdate(ctx, e)
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
		err := p.sender.SendAndUpdate(ctx, envelope)
		if err != nil {
			slog.Warn("failing back to db polling",
				slog.Any("err", err),
				slog.String("id", envelope.IDString()),
			)
		}
	}()
}

func (p *Pump) sendSerial(ctx context.Context, envelope *entity.Envelope) {
	p.wg.Add(1)
	defer p.wg.Done()

	err := p.sender.SendAndUpdate(ctx, envelope)
	if err != nil {
		slog.Warn("failing back to db polling",
			slog.Any("err", err),
			slog.String("id", envelope.IDString()),
		)
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

		h, ok := p.handlers.Load(envelope.Group)
		if !ok {
			slog.Error("no handler found", slog.String("group", envelope.Group))
			return
		}
		handler := h.(Handler)
		err := handler.HandleAndUpdate(ctx, envelope)
		if err != nil {
			slog.Warn("falling back to db polling",
				slog.Any("err", err),
				slog.String("id", envelope.IDString()),
			)
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
			es, err := p.storage.ClaimReceivedBatch(ctx, p.gapOpts.ClaimBatchSize)
			if err != nil {
				slog.Error("failed to claim received messages", slog.Any("err", err))
				wait(ctx, 10*time.Second)
				continue
			}

			for _, e := range es {
				h, ok := p.handlers.Load(e.Group)
				if !ok {
					slog.Error("no handler found", slog.String("group", e.Group))
					continue
				}
				handler := h.(Handler)
				err := handler.HandleAndUpdate(ctx, e)
				if err != nil {
					wait(ctx, 10*time.Second)
				}
			}
		}
	}
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
