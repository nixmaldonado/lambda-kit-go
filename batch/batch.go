// Package batch processes SQS, Kinesis and DynamoDB Streams batches one
// record at a time and reports partial failures, so Lambda retries only the
// records that failed instead of the whole batch.
//
// Turn on ReportBatchItemFailures in the event source mapping
// (FunctionResponseTypes: ReportBatchItemFailures). Without it, Lambda ignores
// the response: a batch with failed records counts as a success and the
// failed records are deleted from the queue or skipped in the stream.
//
// How failures are handled depends on the source:
//
//   - SQS standard queues: every record is processed, and each failed record
//     is reported.
//   - SQS FIFO queues: processing stops at the first failure, and that record
//     and all later ones are reported, so their order is kept.
//   - Kinesis and DynamoDB Streams: processing stops at the first failure and
//     only that record is reported. Lambda retries the stream from it, so the
//     later records are processed again anyway, in order.
//
// A record whose function panics counts as failed; the panic does not crash
// the invocation. If no record succeeds, the handler also returns an error
// matching [ErrAllFailed], so Lambda counts the invocation as failed and backs
// off.
//
// To process each record at most once, call idempotency.Do inside the record
// function.
package batch

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
)

// ErrAllFailed is returned when every record in a non-empty batch failed or
// was not processed.
var ErrAllFailed = errors.New("batch: no record succeeded")

// Option configures a batch handler.
type Option func(*config)

type config struct {
	onError func(ctx context.Context, id string, err error)
}

// OnError registers fn to be called for every record that fails, with the
// record's identifier (message ID or sequence number) and its error. Use it to
// log failures: the handler response only carries identifiers.
func OnError(fn func(ctx context.Context, id string, err error)) Option {
	return func(c *config) {
		c.onError = fn
	}
}

func newConfig(opts []Option) config {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}

	return cfg
}

// mode says what to do after a record fails.
type mode int

const (
	// continueOnFailure processes every record and reports each failure.
	continueOnFailure mode = iota
	// stopReportRest stops at the first failure and reports it and every
	// later record. Used for SQS FIFO queues.
	stopReportRest
	// stopReportFirst stops at the first failure and reports only it. Used
	// for streams, where Lambda retries from the first reported record.
	stopReportFirst
)

// run calls process for each record and returns the identifiers to report as
// failed.
func run[T any](
	ctx context.Context,
	records []T,
	id func(T) string,
	process func(context.Context, T) error,
	m mode,
	cfg config,
) ([]string, error) {
	failed := []string{}
	errs := []error{}
	succeeded := 0

	for idx, rec := range records {
		// Out of time: stop calling process and hand the rest back to Lambda.
		if err := ctx.Err(); err != nil {
			failed = append(failed, stopAt(records[idx:], id, m)...)
			errs = append(errs, err)

			break
		}

		err := safeProcess(ctx, process, rec)
		if err == nil {
			succeeded++

			continue
		}

		recID := id(rec)
		errs = append(errs, fmt.Errorf("record %s: %w", recID, err))
		if cfg.onError != nil {
			cfg.onError(ctx, recID, err)
		}

		if m == continueOnFailure {
			failed = append(failed, recID)

			continue
		}
		failed = append(failed, stopAt(records[idx:], id, m)...)

		break
	}

	if len(records) > 0 && succeeded == 0 {
		return failed, fmt.Errorf("%w: %w", ErrAllFailed, errors.Join(errs...))
	}

	return failed, nil
}

// stopAt returns the identifiers to report when processing stops at rest[0].
func stopAt[T any](rest []T, id func(T) string, m mode) []string {
	if m == stopReportFirst {
		return []string{id(rest[0])}
	}

	ids := make([]string, 0, len(rest))
	for _, rec := range rest {
		ids = append(ids, id(rec))
	}

	return ids
}

// safeProcess calls process and turns a panic into an error, so one bad
// record does not fail the whole batch.
func safeProcess[T any](ctx context.Context, process func(context.Context, T) error, rec T) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("batch: panic: %v\n%s", p, debug.Stack())
		}
	}()

	return process(ctx, rec)
}
