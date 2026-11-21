# SQS orders example

A complete, deployable Lambda that charges orders from an SQS queue **at most once per order**, using the `batch` and `idempotency` packages. It doubles as the end-to-end test that runs the library against real AWS.

```
main.go            the Lambda function (copy this)
terraform/         queue + DLQ, idempotency table, ledger table, function, IAM, trigger
e2e/loadgen/       sends realistic traffic, including the hard cases
e2e/verify/        checks every order was charged exactly once
Makefile           build / deploy / test / destroy
```

## What it builds

| Resource | Notes |
|---|---|
| SQS queue + dead-letter queue | Messages that fail 3 receives go to the DLQ |
| Idempotency table | Partition key `id`, TTL on `expiration`, on-demand |
| Ledger table | One counter per order: where the function "charges" |
| Lambda function | Go, `provided.al2023`, arm64, 10s timeout |
| SQS trigger | Batch size 10, `ReportBatchItemFailures` on, max 10 concurrent instances |
| IAM role | Least privilege: exactly the permissions the main README documents |
| Log group | Created by Terraform with 1-day retention, so `destroy` removes it |

Nothing is reachable from the internet: there's no API Gateway or function URL, and the queue only accepts messages from principals in your own account.

## Run it

Requires Go, Terraform and AWS credentials for an account you're happy to create resources in.

```sh
export AWS_PROFILE=my-sandbox   # pick the account explicitly
make deploy                     # prints the account ID first, then creates 9 resources
make test                       # sends ~1,900 messages and verifies the results (~5 min)
make destroy                    # removes everything and lists anything still tagged
```

A run costs a few cents at most, and nothing while idle. Every resource is tagged `Project=lambda-kit-go-e2e`; `make leftovers` lists anything that survived a teardown. That listing can show deleted resources for a few minutes after `destroy`.

## What the test sends

| Traffic | Why |
|---|---|
| 1,000 orders, once each | Normal traffic |
| 200 orders published 2–4 times, shuffled | Producer duplicates: different message IDs, same order |
| 50 orders sent 5 times at the same instant | Copies land in different batches and Lambda instances at once, racing for the same key |
| 5 slow orders (9.3s on a 10s timeout), each duplicated | Exercises the timeout margin and in-progress duplicates |
| 20 invalid messages | Must fail alone and end up in the DLQ |

The function "charges" an order by adding 1 to a counter in the ledger table. That operation is deliberately not idempotent, so a double charge would show up as `charges = 2`.

## Results from a real run

```
Run rtovb0t: 1879 messages for 1255 orders, 20 poison

  [PASS] no order charged twice (0 doubled)
  [PASS] every order charged (0 missing)
  [PASS] no valid order in the DLQ (0 found)
  [PASS] IAM permissions sufficient (0 AccessDenied lines)

  Evidence the interesting paths ran:
    concurrent duplicates blocked (ErrInProgress):  6
    records handed back near timeout:               19
    invocations that hit the hard timeout:          0
    charged but not recorded:                       0
```

## Using it in your own project

- Copy `main.go` and replace `charge` with your real side effect.
- Remove the `DelayMs` field; it only exists so the test can push an invocation toward its timeout.
- Remove the `replace` line in `go.mod` and depend on a released version of `github.com/nixmaldonado/lambda-kit-go`.
- Set `idempotency.WithExpiry` longer than your queue's retention and any dead-letter queue redrive you might do.
- Keep `ReportBatchItemFailures` on. Without it, Lambda deletes failed messages.

Not using Terraform? The resources above are all the library needs; any tool that creates them works.
