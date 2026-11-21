package batch_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-lambda-go/events"

	"github.com/nixmaldonado/lambda-kit-go/batch"
	"github.com/nixmaldonado/lambda-kit-go/idempotency"
)

type Order struct {
	ID string `json:"id"`
}

// Each order is charged at most once, even when SQS delivers its message
// twice, and only messages that fail are retried.
func ExampleSQS() {
	// In production, use dynamodbstore.New instead.
	idem := idempotency.New(idempotency.NewMemoryStore(), idempotency.WithKeyPrefix("charge"))

	handler := batch.SQS(func(ctx context.Context, msg events.SQSMessage) error {
		var o Order
		if err := json.Unmarshal([]byte(msg.Body), &o); err != nil {
			return err
		}

		_, err := idempotency.Do(ctx, idem, o.ID, func(context.Context) (struct{}, error) {
			fmt.Println("charging", o.ID)

			return struct{}{}, nil
		})
		if errors.Is(err, idempotency.ErrResponseNotSaved) {
			// The charge happened; don't report the message as failed, or
			// its redelivery could charge again. Log it instead.
			return nil
		}

		return err
	})
	// In main: lambda.Start(handler)

	// The same order arrives twice, plus a malformed message.
	ev := events.SQSEvent{Records: []events.SQSMessage{
		{MessageId: "m1", Body: `{"id":"o-1"}`},
		{MessageId: "m2", Body: `{"id":"o-1"}`},
		{MessageId: "m3", Body: `not json`},
	}}

	resp, _ := handler(context.Background(), ev)

	fmt.Println("failed:", resp.BatchItemFailures)
	// Output:
	// charging o-1
	// failed: [{m3}]
}
