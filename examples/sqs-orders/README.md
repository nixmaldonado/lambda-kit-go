# SQS orders example

A complete, deployable Lambda that charges orders from an SQS queue **at most once per order**, using the `batch` and `idempotency` packages. It doubles as the end-to-end test that runs the library against real AWS.

```
main.go            the Lambda function (copy this)
main_test.go       unit tests for the charge headroom guard
terraform/         queue + DLQ, idempotency table, ledger table, function, IAM, trigger
e2e/loadgen/       sends realistic traffic, including the hard cases
e2e/verify/        checks every order was charged exactly once
Makefile           build / deploy / test / destroy
```

## What it builds

| Resource | Notes |
|---|---|
| SQS queue + dead-letter queue | 60s visibility timeout (6x the function timeout) and 5 receives before the DLQ, as AWS recommends for Lambda |
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
make test                       # sends ~1,900 messages and verifies the results (~8 min)
make destroy                    # removes everything and lists anything still tagged
```

A run costs a few cents at most, and nothing while idle. Every resource is tagged `Project=lambda-kit-go-e2e`; `make leftovers` lists anything that survived a teardown. That listing can show deleted resources for a few minutes after `destroy`.

## What the test sends

| Traffic | Why |
|---|---|
| 1,000 orders, once each | Normal traffic |
| 200 orders published 2–4 times, shuffled | Producer duplicates: different message IDs, same order |
| 50 orders sent 5 times at the same instant | Copies land in different batches and Lambda instances at once, racing for the same key |
| 5 slow orders (6s on a 10s timeout), each duplicated | Exercises the charge headroom guard and in-progress duplicates |
| 20 invalid messages | Must fail alone and end up in the DLQ |

The function "charges" an order by adding 1 to a counter in the ledger table. That operation is deliberately not idempotent, so a double charge would show up as `charges = 2`.

## Results from a real run

```
Run rtovc7e: 1899 messages for 1255 orders, 20 poison

  [PASS] no order charged twice (0 doubled)
  [PASS] every order charged (0 missing)
  [PASS] no valid order in the DLQ (0 found)
  [PASS] every invalid message reached the DLQ (20 of 20)
  [PASS] IAM permissions sufficient (0 AccessDenied lines)

  Evidence the interesting paths ran:
    concurrent duplicates blocked (ErrInProgress):  3
    charges deferred by the headroom guard:         0
    records handed back near timeout:               0
    invocations that hit the hard timeout:          0
    charged but not recorded:                       0
```

The run left exactly 1,255 idempotency records, one per order. The slow orders each ran in their own invocation, the longest taking 7.6s, so neither the headroom guard nor the batch timeout margin had to step in this time. An earlier run with 9.3s slow orders and no headroom guard did hand 19 records back near the deadline.

## Using it in your own project

- Copy `main.go` and replace `charge` with your real side effect. Set `chargeHeadroom` to cover your side effect's worst case plus saving the result: don't start an irreversible step without time to finish and record it.
- Remove the `DelayMs` field; it only exists so the test can push an invocation toward its timeout.
- Remove the `replace` line in `go.mod` and depend on a released version of `github.com/nixmaldonado/lambda-kit-go`.
- Set `idempotency.WithExpiry` longer than your queue's retention and any dead-letter queue redrive you might do.
- Keep `ReportBatchItemFailures` on. Without it, Lambda deletes failed messages.
- Alert on `charged but not recorded`. When saving the result fails, the message isn't redelivered, but a separate duplicate arriving after the invocation's deadline would be charged again. The library can't prevent that, because the store is what failed.

Not using Terraform? The resources above are all the library needs; any tool that creates them works.
