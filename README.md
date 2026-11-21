# lambda-kit-go

Utilities for AWS Lambda functions written in Go, inspired by [Powertools for AWS Lambda](https://github.com/aws-powertools).

This is a community project. It is not affiliated with or endorsed by AWS.

## Packages

| Package | What it does |
|---|---|
| [`idempotency`](./idempotency) | Makes handlers safe to retry: repeated events return the saved response instead of running again. |

## Idempotency

```sh
go get github.com/nixmaldonado/lambda-kit-go
```

```go
package main

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/nixmaldonado/lambda-kit-go/idempotency"
	"github.com/nixmaldonado/lambda-kit-go/idempotency/dynamodbstore"
)

type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

type Receipt struct {
	OrderID string `json:"orderId"`
}

func charge(ctx context.Context, o Order) (Receipt, error) {
	// Charge the card. This runs once per order ID.
	return Receipt{OrderID: o.ID}, nil
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}
	store := dynamodbstore.New(dynamodb.NewFromConfig(cfg), "idempotency")
	idem := idempotency.New(store)

	lambda.Start(idempotency.Handler(
		idem,
		charge,
		idempotency.KeyFrom(func(o Order) any { return o.ID }),
	))
}
```

### How it works

1. The key data (the whole event by default, or what `KeyFrom` returns) is JSON-encoded and hashed. The key is `<prefix>#<sha256>`. The prefix defaults to the function name.
2. An `INPROGRESS` record is written with a conditional put. If a live record already exists:
   - `COMPLETED`: the saved response is returned and the handler does not run.
   - `INPROGRESS`: `ErrInProgress` is returned.
3. The handler runs.
   - On success, the response is saved as `COMPLETED` for the expiry window (default 1 hour).
   - On error, the record is deleted so a retry can run again.

If an invocation times out, its `INPROGRESS` record expires at the Lambda deadline, and the next retry takes over. Each record carries the token of the invocation that wrote it, so a slow invocation that finishes late cannot overwrite or delete the record of the one that took over.

> **Always set `KeyFrom` for AWS event types.** API Gateway, SQS and EventBridge events contain fields that change on every delivery (request ID, receipt handle, receive count, timestamps). If the whole event is the key, a retried event never matches and the handler runs again.

### When the response can't be saved

If the handler succeeds but saving its response fails, the wrapper returns the result together with an error matching `idempotency.ErrResponseNotSaved`. The side effect has already happened. Passing that error to Lambda makes it retry, and once the in-progress record expires the retry runs the handler again. To avoid that, check for it:

```go
wrapped := idempotency.Handler(idem, charge, idempotency.KeyFrom(func(o Order) any { return o.ID }))

lambda.Start(func(ctx context.Context, o Order) (Receipt, error) {
	r, err := wrapped(ctx, o)
	if errors.Is(err, idempotency.ErrResponseNotSaved) {
		slog.ErrorContext(ctx, "charged but not recorded", "order", o.ID, "err", err)
		return r, nil // don't let Lambda retry
	}
	return r, err
})
```

### Options

| Option | Purpose |
|---|---|
| `idempotency.WithExpiry(d)` | How long a completed record protects its key. Default 1 hour. Must be positive. |
| `idempotency.WithKeyPrefix(p)` | Key namespace. Default `AWS_LAMBDA_FUNCTION_NAME`. Use different prefixes for different operations in one function. |
| `idempotency.KeyFrom(fn)` | Which part of the event identifies the request. |
| `idempotency.ValidateWith(fn)` | Which part of the event must match on repeats. A mismatch returns `ErrPayloadMismatch`. |

### Any function, not just handlers

```go
idem := idempotency.New(store, idempotency.WithKeyPrefix("send-welcome-email"))

msgID, err := idempotency.Do(ctx, idem, userID, func(ctx context.Context) (string, error) {
	return mailer.Send(ctx, welcomeEmail(userID))
})
```

### DynamoDB table

The table needs a string partition key named `id`. Enable TTL on the `expiration` attribute. The attribute names match Powertools, so an existing Powertools idempotency table works. The store adds one attribute of its own, `lk_token`, for the ownership token.

```yaml
IdempotencyTable:
  Type: AWS::DynamoDB::Table
  Properties:
    BillingMode: PAY_PER_REQUEST
    AttributeDefinitions:
      - AttributeName: id
        AttributeType: S
    KeySchema:
      - AttributeName: id
        KeyType: HASH
    TimeToLiveSpecification:
      AttributeName: expiration
      Enabled: true
```

The function needs `dynamodb:PutItem`, `dynamodb:GetItem` and `dynamodb:DeleteItem` on the table.

### Caveats

- Responses are saved as JSON. Unexported fields are not saved, so a replayed response is missing them.
- Empty key data (`null`, `""`, `{}`, `[]`) returns `ErrMissingKey`, and the handler does not run.
- If the handler errors or panics, the record is deleted so a retry can run. This also happens when the invocation's context was cancelled: saving and cleanup use a context that ignores cancellation, with a 5 second timeout.

## Development

```sh
go test -race ./...

# DynamoDB integration tests
docker run --rm -p 8000:8000 amazon/dynamodb-local
LAMBDAKIT_DYNAMODB_ENDPOINT=http://localhost:8000 go test -race ./...
```

## License

[MIT](./LICENSE)
