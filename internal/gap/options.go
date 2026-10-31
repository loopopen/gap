package gap

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loopopen/gap/broker"
	"github.com/loopopen/gap/dashboard"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/storage"
	"github.com/loopopen/shoot"
)

//go:generate go tool shoot new -opt -short -type=Options,BrokerOptions

type Options struct {
	runtimeID uint64            `new:"-"`
	runtime   *runtimeLifecycle `new:"-"`

	//@def=context.Background()
	Context context.Context

	//@def=context.Background()
	DrainContext context.Context

	ServiceName string

	//@def="v1"
	Version string

	//@def="default"
	DefaultGroup string

	//@def=200
	ClaimBatchSize int

	//@def=30
	MaxRetries int

	//@def=180
	LeaseTTLSeconds int

	//@def=10
	RetryIntervalSeconds int

	//@def=1
	PumpIntervalSeconds int

	MaxPublishConcurrency int

	//@def=-1
	WorkerID int64

	//@def=runtime.GOMAXPROCS(0)*512
	PublishBufferSize int

	//@def=1
	WorkConcurrencyFactor int

	DashboardOptions  *dashboard.Options  `new:"-"`
	StorageOptions    StoragePlugin       `new:"-"`
	HandlerOptsLst    []HandlerOptions    `new:"-"`
	DependencyOptsLst []DependencyOptions `new:"-"`
	Dependencies      []any               `new:"-"`
}

var nextRuntimeID atomic.Uint64

var ErrRuntimeStopping = errors.New("gap: runtime is shutting down")

const maxDurationSeconds int64 = (1<<63 - 1) / int64(time.Second)

type closer interface {
	Close() error
}

type runtimeLifecycle struct {
	intakeCtx    context.Context
	intakeCancel context.CancelFunc
	workCancel   context.CancelFunc

	mu        sync.Mutex
	accepting bool
	active    int
	idle      chan struct{}
	idleOnce  sync.Once
	resources []closer

	closeOnce sync.Once
	closeErr  error
}

// Validate checks options that affect the message state machine before the
// runtime starts any workers or opens storage resources.
func (o *Options) Validate() error {
	if o == nil {
		return errors.New("gap: options must not be nil")
	}

	var errs []error
	errs = append(errs, validatePositiveOption("ClaimBatchSize", o.ClaimBatchSize))
	errs = append(errs, validatePositiveOption("MaxRetries", o.MaxRetries))
	errs = append(errs, validateDurationSeconds("LeaseTTLSeconds", o.LeaseTTLSeconds))
	errs = append(errs, validateDurationSeconds("PumpIntervalSeconds", o.PumpIntervalSeconds))
	return errors.Join(errs...)
}

func validatePositiveOption(field string, value int) error {
	if value <= 0 {
		return fmt.Errorf("gap: invalid %s=%d: must be greater than 0", field, value)
	}
	return nil
}

func validateDurationSeconds(field string, value int) error {
	if err := validatePositiveOption(field, value); err != nil {
		return err
	}
	if int64(value) > maxDurationSeconds {
		return fmt.Errorf("gap: invalid %s=%d: must not exceed %d", field, value, maxDurationSeconds)
	}
	return nil
}

// RuntimeIdentity identifies all option snapshots owned by the same Gap.
// Cloning Options preserves the identity, while independently created runtimes
// receive different identities.
func (o *Options) RuntimeIdentity() uint64 {
	if id := atomic.LoadUint64(&o.runtimeID); id != 0 {
		return id
	}
	id := nextRuntimeID.Add(1)
	if atomic.CompareAndSwapUint64(&o.runtimeID, 0, id) {
		return id
	}
	return atomic.LoadUint64(&o.runtimeID)
}

// InitializeRuntime gives one Gap instance separate intake and work contexts.
// Cancelling the configured parent stops intake immediately, while work remains
// alive until Shutdown either drains or times out.
func (o *Options) InitializeRuntime() {
	parent := o.Context
	if parent == nil {
		parent = context.Background()
	}
	intakeCtx, intakeCancel := context.WithCancel(parent)
	workCtx, workCancel := context.WithCancel(context.WithoutCancel(parent))
	o.runtime = &runtimeLifecycle{
		intakeCtx:    intakeCtx,
		intakeCancel: intakeCancel,
		workCancel:   workCancel,
		accepting:    true,
		idle:         make(chan struct{}),
	}
	o.Context = intakeCtx
	o.DrainContext = workCtx
}

// BeginOperation admits one externally initiated runtime operation. Shutdown
// closes admission before waiting, so no operation can appear after the wait.
func (o *Options) BeginOperation() (func(), error) {
	r := o.runtime
	if r == nil {
		if o.Context != nil {
			if err := o.Context.Err(); err != nil {
				return nil, ErrRuntimeStopping
			}
		}
		return func() {}, nil
	}

	r.mu.Lock()
	if !r.accepting || r.intakeCtx.Err() != nil {
		r.mu.Unlock()
		return nil, ErrRuntimeStopping
	}
	r.active++
	r.mu.Unlock()

	var done atomic.Bool
	return func() {
		if !done.CompareAndSwap(false, true) {
			return
		}
		r.mu.Lock()
		r.active--
		if !r.accepting && r.active == 0 {
			r.idleOnce.Do(func() { close(r.idle) })
		}
		r.mu.Unlock()
	}, nil
}

// OperationContext preserves the caller's cancellation while also allowing a
// timed-out Shutdown to cancel already admitted work.
func (o *Options) OperationContext(ctx context.Context) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	if o.runtime == nil || o.DrainContext == nil {
		return context.WithCancel(ctx)
	}
	opCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(o.DrainContext, cancel)
	return opCtx, func() {
		stop()
		cancel()
	}
}

// StopAccepting closes the external operation gate and stops broker intake and
// database polling. Already admitted work continues on DrainContext.
func (o *Options) StopAccepting() {
	if o.runtime == nil {
		return
	}
	r := o.runtime
	r.mu.Lock()
	if r.accepting {
		r.accepting = false
		r.intakeCancel()
		if r.active == 0 {
			r.idleOnce.Do(func() { close(r.idle) })
		}
	}
	r.mu.Unlock()
}

func (o *Options) WaitOperations(ctx context.Context) error {
	if o.runtime == nil {
		return nil
	}
	select {
	case <-o.runtime.idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *Options) CancelWork() {
	if o.runtime != nil {
		o.runtime.workCancel()
	}
}

func (o *Options) RegisterCloser(resource any) {
	resourceCloser, ok := resource.(closer)
	if !ok || o.runtime == nil {
		return
	}
	o.runtime.mu.Lock()
	o.runtime.resources = append(o.runtime.resources, resourceCloser)
	o.runtime.mu.Unlock()
}

func (o *Options) CloseResources() error {
	if o.runtime == nil {
		return nil
	}
	r := o.runtime
	r.closeOnce.Do(func() {
		r.mu.Lock()
		resources := append([]closer(nil), r.resources...)
		r.resources = nil
		r.mu.Unlock()
		for i := len(resources) - 1; i >= 0; i-- {
			r.closeErr = errors.Join(r.closeErr, resources[i].Close())
		}
	})
	return r.closeErr
}

type BrokerOptions struct {
	// BrokerName is the stable logical name used to route persisted messages.
	// It must not contain connection details such as hosts or credentials.
	BrokerName string

	Plugin BrokerPlugin `new:"-"`
}

// StoragePlugin is the selected storage driver. The concrete options open the
// database themselves, so dispatch does not go through a plugin registry.
type StoragePlugin interface {
	PluginType() enum.Plugin
	OpenStorage(opts *Options, role storage.Role) (storage.Storage, error)
}

// BrokerPlugin is the selected broker driver.
type BrokerPlugin interface {
	PluginType() enum.Plugin
	OpenProducer(opts *Options) (broker.Producer, error)
	OpenConsumer(opts *Options, group string) (broker.Consumer, error)
}

type HandlerOptions struct {
	GroupOptions
	Topic   string
	Handler Handler[[]byte]
}

type GroupOptions struct {
	Group             string
	IngestConcurrency int
}

func (o *Options) LeaseTTL() time.Duration {
	return time.Second * time.Duration(o.LeaseTTLSeconds)
}

func (o *Options) RetryInterval() time.Duration {
	return time.Second * time.Duration(o.RetryIntervalSeconds)
}

func (o *Options) PumpInterval() time.Duration {
	return time.Second * (time.Duration(o.PumpIntervalSeconds))
}

func UseDashboard(opts ...shoot.Option[dashboard.Options, *dashboard.Options]) shoot.Option[Options, *Options] {
	return func(o *Options) {
		options := new(dashboard.Options).With(opts...)
		o.DashboardOptions = options
	}
}

func Inject(values ...any) shoot.Option[Options, *Options] {
	return func(o *Options) {
		o.Dependencies = append(o.Dependencies, values...)
	}
}
