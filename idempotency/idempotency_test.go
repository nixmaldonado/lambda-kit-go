package idempotency

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

type receipt struct {
	OrderID string `json:"orderId"`
	Charged int    `json:"charged"`
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

func newTestIdempotency(t *testing.T, store Store, opts ...Option) (*Idempotency, *clock) {
	t.Helper()

	// Start at the real time so context deadlines derived from it are in the future.
	clk := &clock{now: time.Now()}
	i := New(store, append([]Option{WithKeyPrefix("test")}, opts...)...)
	i.now = clk.Now

	return i, clk
}

// countingHandler returns a handler that charges the order and counts calls.
func countingHandler(calls *atomic.Int32) func(context.Context, order) (receipt, error) {
	return func(_ context.Context, o order) (receipt, error) {
		calls.Add(1)

		return receipt{OrderID: o.ID, Charged: o.Amount}, nil
	}
}

func TestHandlerReplaysSavedResponse(t *testing.T) {
	i, _ := newTestIdempotency(t, NewMemoryStore())
	var calls atomic.Int32
	h := Handler(i, countingHandler(&calls))
	ctx := context.Background()

	first, err := h(ctx, order{ID: "o-1", Amount: 10})
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	second, err := h(ctx, order{ID: "o-1", Amount: 10})
	if err != nil {
		t.Fatalf("second call: %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1", got)
	}
	if first != second {
		t.Errorf("replayed %+v, want %+v", second, first)
	}
}

func TestHandlerDifferentKeysRunSeparately(t *testing.T) {
	i, _ := newTestIdempotency(t, NewMemoryStore())
	var calls atomic.Int32
	h := Handler(i, countingHandler(&calls))
	ctx := context.Background()

	for _, id := range []string{"o-1", "o-2"} {
		if _, err := h(ctx, order{ID: id, Amount: 10}); err != nil {
			t.Fatalf("order %s: %v", id, err)
		}
	}

	if got := calls.Load(); got != 2 {
		t.Errorf("handler ran %d times, want 2", got)
	}
}

func TestKeyFromIgnoresOtherFields(t *testing.T) {
	i, _ := newTestIdempotency(t, NewMemoryStore())
	var calls atomic.Int32
	h := Handler(i, countingHandler(&calls), KeyFrom(func(o order) any { return o.ID }))
	ctx := context.Background()

	if _, err := h(ctx, order{ID: "o-1", Amount: 10}); err != nil {
		t.Fatal(err)
	}
	got, err := h(ctx, order{ID: "o-1", Amount: 99})
	if err != nil {
		t.Fatal(err)
	}

	if calls.Load() != 1 {
		t.Errorf("handler ran %d times, want 1", calls.Load())
	}
	if got.Charged != 10 {
		t.Errorf("Charged = %d, want the saved 10", got.Charged)
	}
}

func TestValidateWithRejectsChangedPayload(t *testing.T) {
	i, _ := newTestIdempotency(t, NewMemoryStore())
	var calls atomic.Int32
	h := Handler(
		i,
		countingHandler(&calls),
		KeyFrom(func(o order) any { return o.ID }),
		ValidateWith(func(o order) any { return o.Amount }),
	)
	ctx := context.Background()

	if _, err := h(ctx, order{ID: "o-1", Amount: 10}); err != nil {
		t.Fatal(err)
	}
	_, err := h(ctx, order{ID: "o-1", Amount: 99})

	if !errors.Is(err, ErrPayloadMismatch) {
		t.Errorf("err = %v, want ErrPayloadMismatch", err)
	}
}

func TestHandlerErrorAllowsRetry(t *testing.T) {
	i, _ := newTestIdempotency(t, NewMemoryStore())
	errBoom := errors.New("boom")
	var calls atomic.Int32
	h := Handler(i, func(_ context.Context, o order) (receipt, error) {
		if calls.Add(1) == 1 {
			return receipt{}, errBoom
		}

		return receipt{OrderID: o.ID}, nil
	})
	ctx := context.Background()

	if _, err := h(ctx, order{ID: "o-1"}); !errors.Is(err, errBoom) {
		t.Fatalf("first call err = %v, want errBoom", err)
	}
	if _, err := h(ctx, order{ID: "o-1"}); err != nil {
		t.Fatalf("retry: %v", err)
	}

	if got := calls.Load(); got != 2 {
		t.Errorf("handler ran %d times, want 2", got)
	}
}

func TestConcurrentCallsRunOnce(t *testing.T) {
	i, _ := newTestIdempotency(t, NewMemoryStore())
	release := make(chan struct{})
	var calls atomic.Int32
	h := Handler(i, func(_ context.Context, o order) (receipt, error) {
		calls.Add(1)
		<-release

		return receipt{OrderID: o.ID}, nil
	})
	ctx := context.Background()

	firstDone := make(chan error)
	go func() {
		_, err := h(ctx, order{ID: "o-1"})
		firstDone <- err
	}()

	// Wait for the first call to claim the key.
	for calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}

	const others = 10
	var wg sync.WaitGroup
	errs := make(chan error, others)
	for range others {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h(ctx, order{ID: "o-1"})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	close(release)

	if err := <-firstDone; err != nil {
		t.Fatalf("first call: %v", err)
	}
	for err := range errs {
		if !errors.Is(err, ErrInProgress) {
			t.Errorf("concurrent call err = %v, want ErrInProgress", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1", got)
	}
}

func TestExpiredRecordRunsAgain(t *testing.T) {
	i, clk := newTestIdempotency(t, NewMemoryStore(), WithExpiry(time.Minute))
	var calls atomic.Int32
	h := Handler(i, countingHandler(&calls))
	ctx := context.Background()

	if _, err := h(ctx, order{ID: "o-1"}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Minute)
	if _, err := h(ctx, order{ID: "o-1"}); err != nil {
		t.Fatal(err)
	}

	if got := calls.Load(); got != 2 {
		t.Errorf("handler ran %d times, want 2", got)
	}
}

func TestAbandonedInProgressRecordIsTakenOver(t *testing.T) {
	store := NewMemoryStore()
	i, clk := newTestIdempotency(t, store)
	key, err := i.key(order{ID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	// An invocation that timed out, leaving its record in progress.
	abandoned := Record{
		Key:                 key,
		Status:              StatusInProgress,
		ExpiresAt:           clk.Now().Add(time.Hour),
		InProgressExpiresAt: clk.Now().Add(time.Second),
	}
	if err := store.Create(context.Background(), abandoned, clk.Now()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	h := Handler(i, countingHandler(&calls))

	if _, err := h(context.Background(), order{ID: "o-1"}); !errors.Is(err, ErrInProgress) {
		t.Fatalf("before deadline err = %v, want ErrInProgress", err)
	}
	clk.Advance(time.Second)
	if _, err := h(context.Background(), order{ID: "o-1"}); err != nil {
		t.Fatalf("after deadline: %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1", got)
	}
}

func TestInProgressExpiryUsesContextDeadline(t *testing.T) {
	store := &recordingStore{Store: NewMemoryStore()}
	i, clk := newTestIdempotency(t, store)
	deadline := clk.Now().Add(15 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	if _, err := Do(ctx, i, "k", func(context.Context) (int, error) { return 1, nil }); err != nil {
		t.Fatal(err)
	}

	if got := store.created.InProgressExpiresAt; !got.Equal(deadline) {
		t.Errorf("InProgressExpiresAt = %v, want %v", got, deadline)
	}
}

func TestMissingKey(t *testing.T) {
	i, _ := newTestIdempotency(t, NewMemoryStore())
	var calls atomic.Int32
	h := Handler(i, countingHandler(&calls), KeyFrom(func(o order) any { return o.ID }))

	_, err := h(context.Background(), order{})

	if !errors.Is(err, ErrMissingKey) {
		t.Errorf("err = %v, want ErrMissingKey", err)
	}
	if calls.Load() != 0 {
		t.Error("handler ran without a key")
	}
}

func TestKeysAreNamespacedByPrefix(t *testing.T) {
	store := NewMemoryStore()
	a, _ := newTestIdempotency(t, store, WithKeyPrefix("charge"))
	b, _ := newTestIdempotency(t, store, WithKeyPrefix("refund"))
	var calls atomic.Int32
	fn := func(context.Context) (int, error) { return int(calls.Add(1)), nil }
	ctx := context.Background()

	if _, err := Do(ctx, a, "o-1", fn); err != nil {
		t.Fatal(err)
	}
	if _, err := Do(ctx, b, "o-1", fn); err != nil {
		t.Fatal(err)
	}

	if got := calls.Load(); got != 2 {
		t.Errorf("fn ran %d times, want 2", got)
	}
}

func TestSaveFailureIsReported(t *testing.T) {
	errDown := errors.New("store down")
	store := &recordingStore{Store: NewMemoryStore(), updateErr: errDown}
	i, _ := newTestIdempotency(t, store)

	got, err := Do(context.Background(), i, "k", func(context.Context) (int, error) { return 7, nil })

	if !errors.Is(err, errDown) || !errors.Is(err, ErrResponseNotSaved) {
		t.Errorf("err = %v, want errDown and ErrResponseNotSaved", err)
	}
	if got != 7 {
		t.Errorf("result = %d, want 7 even when saving fails", got)
	}
}

func TestCreateFailureDoesNotRun(t *testing.T) {
	errDown := errors.New("store down")
	store := &recordingStore{Store: NewMemoryStore(), createErr: errDown}
	i, _ := newTestIdempotency(t, store)
	ran := false

	_, err := Do(context.Background(), i, "k", func(context.Context) (int, error) {
		ran = true

		return 0, nil
	})

	if !errors.Is(err, errDown) {
		t.Errorf("err = %v, want errDown", err)
	}
	if ran {
		t.Error("fn ran although the record could not be created")
	}
}

type recordingStore struct {
	Store
	created   Record
	createErr error
	updateErr error
	// honorCtx makes Update and Delete fail on a done context, like a real
	// network client.
	honorCtx bool
}

func (s *recordingStore) Create(ctx context.Context, r Record, now time.Time) error {
	if s.createErr != nil {
		return s.createErr
	}
	s.created = r

	return s.Store.Create(ctx, r, now)
}

func (s *recordingStore) Update(ctx context.Context, r Record) error {
	if s.updateErr != nil {
		return s.updateErr
	}
	if s.honorCtx && ctx.Err() != nil {
		return ctx.Err()
	}

	return s.Store.Update(ctx, r)
}

func (s *recordingStore) Delete(ctx context.Context, r Record) error {
	if s.honorCtx && ctx.Err() != nil {
		return ctx.Err()
	}

	return s.Store.Delete(ctx, r)
}

func TestSaveFailureKeepsKeyBlocked(t *testing.T) {
	store := &recordingStore{Store: NewMemoryStore(), updateErr: errors.New("store down")}
	i, _ := newTestIdempotency(t, store)
	var calls atomic.Int32
	fn := func(context.Context) (int, error) { return int(calls.Add(1)), nil }

	if _, err := Do(context.Background(), i, "k", fn); !errors.Is(err, ErrResponseNotSaved) {
		t.Fatalf("err = %v, want ErrResponseNotSaved", err)
	}
	store.updateErr = nil
	_, err := Do(context.Background(), i, "k", fn)

	if !errors.Is(err, ErrInProgress) {
		t.Errorf("retry err = %v, want ErrInProgress", err)
	}
	if calls.Load() != 1 {
		t.Errorf("fn ran %d times, want 1", calls.Load())
	}
}

func TestUnencodableResponseIsNotSaved(t *testing.T) {
	i, _ := newTestIdempotency(t, NewMemoryStore())

	_, err := Do(context.Background(), i, "k", func(context.Context) (func(), error) {
		return func() {}, nil
	})

	if !errors.Is(err, ErrResponseNotSaved) {
		t.Errorf("err = %v, want ErrResponseNotSaved", err)
	}
}

// slowThenTakeover runs a first invocation whose fn outlives its in-progress
// expiry; while it is still running, a second invocation takes over the key
// and completes. It returns the first invocation's error.
func slowThenTakeover(t *testing.T, firstErr error) (*Idempotency, error) {
	t.Helper()

	i, clk := newTestIdempotency(t, NewMemoryStore())
	ctx, cancel := context.WithDeadline(context.Background(), clk.Now().Add(time.Second))
	defer cancel()

	_, err := Do(ctx, i, "k", func(context.Context) (string, error) {
		clk.Advance(2 * time.Second) // past the first invocation's deadline

		got, err := Do(context.Background(), i, "k", func(context.Context) (string, error) {
			return "second", nil
		})
		if err != nil || got != "second" {
			t.Fatalf("takeover = %q, %v", got, err)
		}

		return "first", firstErr
	})

	return i, err
}

func TestSlowInvocationDoesNotOverwriteTakeover(t *testing.T) {
	i, err := slowThenTakeover(t, nil)

	if !errors.Is(err, ErrResponseNotSaved) || !errors.Is(err, ErrNotOwner) {
		t.Errorf("slow invocation err = %v, want ErrResponseNotSaved and ErrNotOwner", err)
	}
	got, err := Do(context.Background(), i, "k", func(context.Context) (string, error) {
		return "third", nil
	})
	if err != nil || got != "second" {
		t.Errorf("replay = %q, %v; want the takeover's \"second\"", got, err)
	}
}

func TestSlowFailingInvocationDoesNotDeleteTakeover(t *testing.T) {
	errBoom := errors.New("boom")
	i, err := slowThenTakeover(t, errBoom)

	if !errors.Is(err, errBoom) || errors.Is(err, ErrNotOwner) {
		t.Errorf("slow invocation err = %v, want only errBoom", err)
	}
	got, err := Do(context.Background(), i, "k", func(context.Context) (string, error) {
		return "third", nil
	})
	if err != nil || got != "second" {
		t.Errorf("replay = %q, %v; want the takeover's \"second\"", got, err)
	}
}

func TestCancelledContextStillReleasesKey(t *testing.T) {
	store := &recordingStore{Store: NewMemoryStore(), honorCtx: true}
	i, _ := newTestIdempotency(t, store)
	ctx, cancel := context.WithCancel(context.Background())

	_, err := Do(ctx, i, "k", func(ctx context.Context) (int, error) {
		cancel()

		return 0, ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	if _, err := Do(context.Background(), i, "k", func(context.Context) (int, error) { return 1, nil }); err != nil {
		t.Errorf("retry after cancel: %v", err)
	}
}

func TestCancelledContextStillSavesResponse(t *testing.T) {
	store := &recordingStore{Store: NewMemoryStore(), honorCtx: true}
	i, _ := newTestIdempotency(t, store)
	ctx, cancel := context.WithCancel(context.Background())

	if _, err := Do(ctx, i, "k", func(context.Context) (int, error) {
		cancel()

		return 1, nil
	}); err != nil {
		t.Fatalf("first call: %v", err)
	}

	got, err := Do(context.Background(), i, "k", func(context.Context) (int, error) { return 2, nil })
	if err != nil || got != 1 {
		t.Errorf("replay = %d, %v; want 1, nil", got, err)
	}
}

func TestPanicReleasesKey(t *testing.T) {
	i, _ := newTestIdempotency(t, NewMemoryStore())

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic did not propagate")
			}
		}()
		_, _ = Do(context.Background(), i, "k", func(context.Context) (int, error) {
			panic("boom")
		})
	}()

	if _, err := Do(context.Background(), i, "k", func(context.Context) (int, error) { return 1, nil }); err != nil {
		t.Errorf("retry after panic: %v", err)
	}
}

func TestWithExpiryRejectsNonPositive(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("WithExpiry(%v) did not panic", d)
				}
			}()
			WithExpiry(d)
		}()
	}
}
