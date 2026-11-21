// Command sqs-orders is a Lambda function that charges orders from an SQS
// queue at most once per order, using the batch and idempotency packages.
//
// It reads two environment variables: IDEMPOTENCY_TABLE (the idempotency
// records) and LEDGER_TABLE (where charges are recorded).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/nixmaldonado/lambda-kit-go/batch"
	"github.com/nixmaldonado/lambda-kit-go/idempotency"
	"github.com/nixmaldonado/lambda-kit-go/idempotency/dynamodbstore"
)

// chargeHeadroom is the time that must be left, on top of the work itself,
// to make the charge and save its result before the Lambda deadline. Without
// it, a charge could start so late that it happens but is never recorded,
// and the retry would charge again.
const chargeHeadroom = 2 * time.Second

// errNotEnoughTime is returned before charging when the invocation is too
// close to its deadline. The message is retried in a fresh invocation.
var errNotEnoughTime = errors.New("not enough time left to charge safely")

// Order is the message body.
type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`

	// DelayMs makes the charge sleep first. It exists only so the end-to-end
	// test can push an invocation toward its timeout; drop it in your code.
	DelayMs int `json:"delayMs,omitempty"`
}

// ledgerAPI is the part of the DynamoDB client that charge uses, so tests
// can count charges without AWS.
type ledgerAPI interface {
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

type app struct {
	idem   *idempotency.Idempotency
	ddb    ledgerAPI
	ledger string
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		slog.Error("loading AWS config", "err", err)
		os.Exit(1)
	}
	ddb := dynamodb.NewFromConfig(cfg)

	a := &app{
		idem: idempotency.New(
			dynamodbstore.New(ddb, mustEnv("IDEMPOTENCY_TABLE")),
			idempotency.WithKeyPrefix("charge"),
			// Longer than the queue's retention, so a late redelivery is
			// still recognized as a duplicate.
			idempotency.WithExpiry(48*time.Hour),
		),
		ddb:    ddb,
		ledger: mustEnv("LEDGER_TABLE"),
	}

	lambda.Start(batch.SQS(a.process, batch.OnError(logFailure)))
}

// process handles one SQS message.
func (a *app) process(ctx context.Context, msg events.SQSMessage) error {
	var o Order
	if err := json.Unmarshal([]byte(msg.Body), &o); err != nil {
		return fmt.Errorf("decoding order: %w", err)
	}
	if o.ID == "" {
		return errors.New("order has no id")
	}

	// Key on the order ID, not the message ID: a producer that publishes
	// the same order twice creates two different message IDs.
	_, err := idempotency.Do(ctx, a.idem, o.ID, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, a.charge(ctx, o)
	})
	if errors.Is(err, idempotency.ErrResponseNotSaved) {
		// The charge happened; only saving the result failed. Reporting the
		// message as failed would redeliver it and charge again. This is not
		// a full guarantee: the record stays in progress only until this
		// invocation's deadline, so a producer duplicate arriving after
		// that would still be charged. Alert on this log line.
		slog.ErrorContext(ctx, "charged but not recorded", "order", o.ID, "err", err)

		return nil
	}

	return err
}

// charge stands in for a real payment call. It adds to a counter in the
// ledger table, which is deliberately not idempotent: a double charge shows
// up as charges = 2.
func (a *app) charge(ctx context.Context, o Order) error {
	// Don't start an irreversible step without time to finish and record it.
	work := time.Duration(o.DelayMs) * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < work+chargeHeadroom {
		return errNotEnoughTime
	}

	if o.DelayMs > 0 {
		select {
		case <-time.After(time.Duration(o.DelayMs) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	_, err := a.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(a.ledger),
		Key: map[string]types.AttributeValue{
			"order_id": &types.AttributeValueMemberS{Value: o.ID},
		},
		UpdateExpression: aws.String("ADD charges :one, charged_amount :amount"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one":    &types.AttributeValueMemberN{Value: "1"},
			":amount": &types.AttributeValueMemberN{Value: strconv.Itoa(o.Amount)},
		},
	})
	if err != nil {
		return fmt.Errorf("recording charge: %w", err)
	}

	return nil
}

func logFailure(ctx context.Context, id string, err error) {
	slog.WarnContext(ctx, "record failed", "messageId", id, "err", err.Error())
}

func mustEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		slog.Error("missing environment variable", "name", name)
		os.Exit(1)
	}

	return v
}
