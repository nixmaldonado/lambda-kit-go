package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/nixmaldonado/lambda-kit-go/idempotency"
)

// fakeLedger counts charges per order instead of calling DynamoDB.
type fakeLedger struct {
	mu      sync.Mutex
	charges map[string]int
}

func (f *fakeLedger) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := in.Key["order_id"].(*types.AttributeValueMemberS).Value
	f.charges[id]++

	return &dynamodb.UpdateItemOutput{}, nil
}

func (f *fakeLedger) count(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.charges[id]
}

func newTestApp() (*app, *fakeLedger) {
	ledger := &fakeLedger{charges: map[string]int{}}
	a := &app{
		idem:   idempotency.New(idempotency.NewMemoryStore(), idempotency.WithKeyPrefix("charge")),
		ddb:    ledger,
		ledger: "ledger",
	}

	return a, ledger
}

// withTimeLeft returns a context whose deadline is d away, like a Lambda
// invocation with d remaining.
func withTimeLeft(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)

	return ctx
}

func TestChargeHeadroomGuard(t *testing.T) {
	tests := []struct {
		name     string
		timeLeft time.Duration // 0 means no deadline
		delayMs  int
		wantErr  error
		charged  int
	}{
		{name: "fast order, plenty of time", timeLeft: 10 * time.Second, charged: 1},
		{name: "fast order, less than the headroom left", timeLeft: chargeHeadroom - 500*time.Millisecond, wantErr: errNotEnoughTime},
		{name: "slow order, work plus headroom doesn't fit", timeLeft: 7 * time.Second, delayMs: 6000, wantErr: errNotEnoughTime},
		{name: "slow order, work plus headroom fits", timeLeft: chargeHeadroom + 1500*time.Millisecond, delayMs: 1000, charged: 1},
		{name: "no deadline", charged: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, ledger := newTestApp()
			ctx := context.Background()
			if tt.timeLeft > 0 {
				ctx = withTimeLeft(t, tt.timeLeft)
			}

			start := time.Now()
			err := a.charge(ctx, Order{ID: "order-42", Amount: 10, DelayMs: tt.delayMs})

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("charge() error = %v, want %v", err, tt.wantErr)
			}
			if got := ledger.count("order-42"); got != tt.charged {
				t.Fatalf("charges = %d, want %d", got, tt.charged)
			}
			// A refusal must happen up front, not after doing the work.
			if tt.wantErr != nil && time.Since(start) > 100*time.Millisecond {
				t.Fatalf("refusal took %v; the guard should refuse before starting", time.Since(start))
			}
		})
	}
}

// TestRefusedChargeIsRetried checks the whole path: a message refused by the
// guard is reported as failed, its idempotency record is released, and the
// redelivery in a fresh invocation charges exactly once.
func TestRefusedChargeIsRetried(t *testing.T) {
	a, ledger := newTestApp()
	body, err := json.Marshal(Order{ID: "order-42", Amount: 10, DelayMs: 200})
	if err != nil {
		t.Fatal(err)
	}
	msg := events.SQSMessage{MessageId: "m1", Body: string(body)}

	// First delivery lands late in a batch, with too little time left.
	if err := a.process(withTimeLeft(t, time.Second), msg); !errors.Is(err, errNotEnoughTime) {
		t.Fatalf("late delivery: error = %v, want %v", err, errNotEnoughTime)
	}
	if got := ledger.count("order-42"); got != 0 {
		t.Fatalf("late delivery charged %d times, want 0", got)
	}

	// Redelivery in a fresh invocation must not be blocked by the first
	// attempt's record, and charges once.
	if err := a.process(withTimeLeft(t, 10*time.Second), msg); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	// A later duplicate replays the saved result.
	if err := a.process(withTimeLeft(t, 10*time.Second), msg); err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	if got := ledger.count("order-42"); got != 1 {
		t.Fatalf("charges = %d, want 1", got)
	}
}
