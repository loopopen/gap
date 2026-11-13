package internal

import (
	"context"
	"sync"
	"time"
)

type addedEntry struct {
	stream string
	maxLen int64
	values map[string]any
}

type fakeStreams struct {
	mu sync.Mutex

	groups    map[string]string
	pending   []streamEntry
	fresh     []streamEntry
	claims    []streamEntry
	claimNext string
	reads     []string
	acks      []string
	adds      []addedEntry
	ackErrs   []error
	claimErr  error
	readErr   error
	pingErr   error
	groupErr  error
}

func newFakeStreams() *fakeStreams {
	return &fakeStreams{groups: make(map[string]string)}
}

func (f *fakeStreams) Ping(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pingErr
}

func (f *fakeStreams) EnsureGroup(_ context.Context, stream, group, start string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.groupErr != nil {
		return f.groupErr
	}
	f.groups[stream+"\x00"+group] = start
	return nil
}

func (f *fakeStreams) Add(_ context.Context, stream string, maxLen int64, values map[string]any) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adds = append(f.adds, addedEntry{stream: stream, maxLen: maxLen, values: values})
	return "1-0", nil
}

func (f *fakeStreams) ReadGroup(ctx context.Context, _, _ string, _ []string, id string, count int64, block time.Duration) ([]streamEntry, error) {
	f.mu.Lock()
	f.reads = append(f.reads, id)
	if f.readErr != nil {
		err := f.readErr
		f.readErr = nil
		f.mu.Unlock()
		return nil, err
	}
	if id == readPending {
		out := append([]streamEntry(nil), f.pending...)
		f.pending = nil
		f.mu.Unlock()
		return out, nil
	}
	if len(f.fresh) > 0 {
		n := len(f.fresh)
		if count > 0 && int(count) < n {
			n = int(count)
		}
		out := append([]streamEntry(nil), f.fresh[:n]...)
		f.fresh = append([]streamEntry(nil), f.fresh[n:]...)
		f.mu.Unlock()
		return out, nil
	}
	f.mu.Unlock()

	if block < 0 {
		return nil, nil
	}
	timer := time.NewTimer(block)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, nil
	}
}

func (f *fakeStreams) Ack(_ context.Context, _, _, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, id)
	if len(f.ackErrs) == 0 {
		return nil
	}
	err := f.ackErrs[0]
	f.ackErrs = f.ackErrs[1:]
	return err
}

func (f *fakeStreams) AutoClaim(context.Context, string, string, string, string, time.Duration, int64) ([]streamEntry, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		err := f.claimErr
		f.claimErr = nil
		return nil, "", err
	}
	out := append([]streamEntry(nil), f.claims...)
	f.claims = nil
	next := f.claimNext
	if next == "" {
		next = claimStart
	}
	return out, next, nil
}

func (f *fakeStreams) snapshot() (reads []string, acks []string, groups map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	groups = make(map[string]string, len(f.groups))
	for key, start := range f.groups {
		groups[key] = start
	}
	return append([]string(nil), f.reads...), append([]string(nil), f.acks...), groups
}
