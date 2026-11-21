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
// When the invocation gets within the timeout margin of its deadline (one
// second by default, see [WithTimeoutMargin]), the remaining records are
// reported as failed without being processed. That leaves time to return the
// response, so records that already succeeded are not retried.
//
// To process each record at most once, call idempotency.Do inside the record
// function.
package batch

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"
)

var (
	// ErrAllFailed is returned when every record in a non-empty batch failed
	// or was not processed.
	ErrAllFailed = errors.New("batch: no record succeeded")

	// ErrNotProcessed is passed to [OnError] for records reported as failed
	// without being processed: after an earlier failure in a FIFO queue, or
	// when the invocation ran out of time.
	ErrNotProcessed = errors.New("batch: record not processed")

	errDeadlineNear = errors.New("invocation deadline is within the timeout margin")
)

const (
	defaultTimeoutMargin = time.Second

	// maxJoinedErrors bounds how many record errors the ErrAllFailed error
	// carries, so a huge batch cannot produce a huge error.
	maxJoinedErrors = 3
)

// Option configures a batch handler.
type Option func(*config)

type config struct {
	onError       func(ctx context.Context, id string, err error)
	timeoutMargin time.Duration
	now           func() time.Time
}

// OnError registers fn to be called for every record reported as failed,
// with the record's identifier (message ID or sequence number) and its error.
// Records reported without being processed get an error matching
// [ErrNotProcessed]. Use it to log failures: the handler response only
// carries identifiers. A panic in fn is ignored.
func OnError(fn func(ctx context.Context, id string, err error)) Option {
	return func(c *config) {
		c.onError = fn
	}
}

// WithTimeoutMargin sets how long before the invocation deadline the handler
// stops starting new records, leaving time to return the response. The
// default is one second. Zero stops only once the deadline has passed, which
// is too late to respond. It panics if d is negative.
func WithTimeoutMargin(d time.Duration) Option {
	if d < 0 {
		panic(fmt.Sprintf("batch: timeout margin must not be negative, got %v", d))
	}

	return func(c *config) {
		c.timeoutMargin = d
	}
}

func newConfig(opts []Option) config {
	cfg := config{
		timeoutMargin: defaultTimeoutMargin,
		now:           time.Now,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	return cfg
}

// outOfTime returns why no new record should start, or nil.
func (c config) outOfTime(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !c.now().Add(c.timeoutMargin).Before(deadline) {
		return errDeadlineNear
	}

	return nil
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

// outcome collects the result of a batch.
type outcome struct {
	cfg       config
	failed    []string
	errs      []error
	omitted   int
	succeeded int
}

// fail reports one record as failed.
func (o *outcome) fail(ctx context.Context, id string, err error) {
	o.failed = append(o.failed, id)
	if len(o.errs) < maxJoinedErrors {
		o.errs = append(o.errs, fmt.Errorf("record %s: %w", id, err))
	} else {
		o.omitted++
	}
	if o.cfg.onError != nil {
		callSafely(func() { o.cfg.onError(ctx, id, err) })
	}
}

// skip reports records as failed without processing them.
func (o *outcome) skip(ctx context.Context, ids []string, reason error) {
	for _, id := range ids {
		o.fail(ctx, id, fmt.Errorf("%w: %w", ErrNotProcessed, reason))
	}
}

func (o *outcome) err(total int) error {
	if total == 0 || o.succeeded > 0 {
		return nil
	}

	joined := errors.Join(o.errs...)
	if o.omitted > 0 {
		return fmt.Errorf("%w: %w\n(and %d more)", ErrAllFailed, joined, o.omitted)
	}

	return fmt.Errorf("%w: %w", ErrAllFailed, joined)
}

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
	o := outcome{cfg: cfg, failed: []string{}}

	for idx, rec := range records {
		if reason := cfg.outOfTime(ctx); reason != nil {
			rest := records[idx:]
			if m == stopReportFirst {
				rest = rest[:1]
			}
			o.skip(ctx, ids(rest, id), reason)

			break
		}

		err := safeProcess(ctx, process, rec)
		if err == nil {
			o.succeeded++

			continue
		}

		recID := id(rec)
		o.fail(ctx, recID, err)
		if m == continueOnFailure {
			continue
		}
		if m == stopReportRest {
			o.skip(ctx, ids(records[idx+1:], id), fmt.Errorf("earlier record %s failed", recID))
		}

		break
	}

	return o.failed, o.err(len(records))
}

func ids[T any](records []T, id func(T) string) []string {
	out := make([]string, 0, len(records))
	for _, rec := range records {
		out = append(out, id(rec))
	}

	return out
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

// callSafely runs fn and discards a panic, so a failing log callback cannot
// crash the invocation.
func callSafely(fn func()) {
	defer func() { _ = recover() }()
	fn()
}
