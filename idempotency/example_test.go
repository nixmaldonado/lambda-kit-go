package idempotency_test

import (
	"context"
	"fmt"

	"github.com/nixmaldonado/lambda-kit-go/idempotency"
)

type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

type Receipt struct {
	OrderID string `json:"orderId"`
	Charged int    `json:"charged"`
}

func ExampleHandler() {
	// In production, use dynamodbstore.New instead.
	idem := idempotency.New(idempotency.NewMemoryStore())

	charge := func(_ context.Context, o Order) (Receipt, error) {
		fmt.Println("charging", o.ID)

		return Receipt{OrderID: o.ID, Charged: o.Amount}, nil
	}

	handler := idempotency.Handler(
		idem,
		charge,
		idempotency.KeyFrom(func(o Order) any { return o.ID }),
	)
	// In main: lambda.Start(handler)

	ctx := context.Background()
	first, _ := handler(ctx, Order{ID: "o-1", Amount: 42})
	second, _ := handler(ctx, Order{ID: "o-1", Amount: 42}) // replayed, not charged again

	fmt.Println(first == second)
	// Output:
	// charging o-1
	// true
}

func ExampleDo() {
	idem := idempotency.New(idempotency.NewMemoryStore(), idempotency.WithKeyPrefix("send-email"))
	ctx := context.Background()

	for range 2 {
		id, _ := idempotency.Do(ctx, idem, "welcome:user-7", func(context.Context) (string, error) {
			fmt.Println("sending")

			return "msg-123", nil
		})
		fmt.Println(id)
	}
	// Output:
	// sending
	// msg-123
	// msg-123
}
