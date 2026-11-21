// Package idempotency makes AWS Lambda handlers, and other functions, safe to
// retry.
//
// The first call for a key runs the function and saves its JSON-encoded
// result. Later calls with the same key, until the record expires, return the
// saved result without running the function. A call that arrives while the
// first one is still running gets [ErrInProgress]. When the function returns
// an error, the record is deleted so a retry can run it again.
//
// Records live in a [Store]. Use [NewMemoryStore] for tests and the
// dynamodbstore subpackage in production.
package idempotency

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

const (
	defaultExpiry = time.Hour
	defaultPrefix = "lambda-kit"
)

// Idempotency holds the settings shared by [Handler] and [Do]. Use a separate
// Idempotency, or a different [WithKeyPrefix], for each operation that should
// have its own keys.
type Idempotency struct {
	store  Store
	expiry time.Duration
	prefix string
	now    func() time.Time
}

// Option configures an [Idempotency].
type Option func(*Idempotency)

// WithExpiry sets how long a completed record protects its key. The default
// is one hour. It panics if d is not positive, since a zero expiry would
// silently disable idempotency.
func WithExpiry(d time.Duration) Option {
	if d <= 0 {
		panic(fmt.Sprintf("idempotency: expiry must be positive, got %v", d))
	}

	return func(i *Idempotency) {
		i.expiry = d
	}
}

// WithKeyPrefix sets the namespace prepended to every key. The default is the
// AWS_LAMBDA_FUNCTION_NAME environment variable, or "lambda-kit" outside
// Lambda.
func WithKeyPrefix(prefix string) Option {
	return func(i *Idempotency) {
		i.prefix = prefix
	}
}

// New returns an Idempotency that saves records in store. It panics if store
// is nil.
func New(store Store, opts ...Option) *Idempotency {
	if store == nil {
		panic("idempotency: nil store")
	}

	prefix := defaultPrefix
	if name := os.Getenv("AWS_LAMBDA_FUNCTION_NAME"); name != "" {
		prefix = name
	}

	i := &Idempotency{
		store:  store,
		expiry: defaultExpiry,
		prefix: prefix,
		now:    time.Now,
	}
	for _, opt := range opts {
		opt(i)
	}

	return i
}

// HandlerOption configures [Handler].
type HandlerOption[T any] func(*handlerConfig[T])

type handlerConfig[T any] struct {
	key     func(T) any
	payload func(T) any
}

// KeyFrom selects the part of the event that identifies a request, such as an
// order ID or an Idempotency-Key header. The value is JSON-encoded and hashed.
// Without KeyFrom, the whole event is the key; see [Handler] for why that is
// rarely what you want.
func KeyFrom[T any](fn func(event T) any) HandlerOption[T] {
	return func(c *handlerConfig[T]) {
		c.key = fn
	}
}

// ValidateWith selects the part of the event that must match on repeated
// requests. A request that reuses a key with a different payload gets
// [ErrPayloadMismatch] instead of the saved response.
func ValidateWith[T any](fn func(event T) any) HandlerOption[T] {
	return func(c *handlerConfig[T]) {
		c.payload = fn
	}
}

// Handler wraps a Lambda handler so that repeated events return the saved
// response instead of running h again.
//
// Use [KeyFrom] with AWS event types. Without it the whole event is the key,
// and API Gateway, SQS and EventBridge events carry fields that change on
// every delivery (request ID, receipt handle, receive count, timestamps), so
// a retried event never matches and the handler runs again.
//
// If h succeeds but its response cannot be saved, the wrapper returns the
// result together with an error matching [ErrResponseNotSaved]. Passing that
// error to Lambda causes a retry; check for it to decide otherwise.
//
// The response type R must survive a JSON round trip: unexported fields are
// not saved.
func Handler[T, R any](
	i *Idempotency,
	h func(context.Context, T) (R, error),
	opts ...HandlerOption[T],
) func(context.Context, T) (R, error) {
	cfg := handlerConfig[T]{
		key: func(event T) any { return event },
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	return func(ctx context.Context, event T) (R, error) {
		c := call{key: cfg.key(event)}
		if cfg.payload != nil {
			c.payload = cfg.payload(event)
			c.validate = true
		}

		return execute(ctx, i, c, func(ctx context.Context) (R, error) {
			return h(ctx, event)
		})
	}
}

// Do runs fn at most once per key until the record expires. key is
// JSON-encoded and hashed. Repeated calls return the saved result.
//
// The result type R must survive a JSON round trip: unexported fields are not
// saved.
func Do[R any](
	ctx context.Context,
	i *Idempotency,
	key any,
	fn func(context.Context) (R, error),
) (R, error) {
	return execute(ctx, i, call{key: key}, fn)
}

type call struct {
	key      any
	payload  any
	validate bool
}

func execute[R any](
	ctx context.Context,
	i *Idempotency,
	c call,
	fn func(context.Context) (R, error),
) (R, error) {
	var zero R

	key, err := i.key(c.key)
	if err != nil {
		return zero, err
	}

	var payloadHash string
	if c.validate {
		payloadHash, err = hashJSON(c.payload)
		if err != nil {
			return zero, fmt.Errorf("idempotency: encoding payload: %w", err)
		}
	}

	now := i.now()
	rec := Record{
		Key:                 key,
		Status:              StatusInProgress,
		ExpiresAt:           now.Add(i.expiry),
		InProgressExpiresAt: inProgressExpiry(ctx, now, i.expiry),
		PayloadHash:         payloadHash,
		Token:               rand.Text(),
	}

	err = i.store.Create(ctx, rec, now)
	var exists *AlreadyExistsError
	switch {
	case errors.As(err, &exists):
		return replay[R](exists.Record, payloadHash)
	case err != nil:
		return zero, fmt.Errorf("idempotency: creating record: %w", err)
	}

	finished := false
	defer func() {
		if !finished {
			// fn panicked or called runtime.Goexit. Free the key so a retry
			// can run; the panic keeps propagating.
			_ = i.release(ctx, rec, nil)
		}
	}()
	result, fnErr := fn(ctx)
	finished = true

	if fnErr != nil {
		return result, i.release(ctx, rec, fnErr)
	}

	// From here on fn has run, so failures keep the in-progress record: it
	// blocks retries until it is abandoned instead of inviting a rerun now.
	data, err := json.Marshal(result)
	if err != nil {
		return result, fmt.Errorf("%w: encoding response: %w", ErrResponseNotSaved, err)
	}

	rec.Status = StatusCompleted
	rec.Response = data
	rec.ExpiresAt = i.now().Add(i.expiry)
	rec.InProgressExpiresAt = time.Time{}

	cleanupCtx, cancel := cleanupContext(ctx)
	defer cancel()
	if err := i.store.Update(cleanupCtx, rec); err != nil {
		return result, fmt.Errorf("%w: %w", ErrResponseNotSaved, err)
	}

	return result, nil
}

// cleanupTimeout bounds the store calls made after fn returns.
const cleanupTimeout = 5 * time.Second

// cleanupContext returns a context for the store calls made after fn returns.
// It survives cancellation of ctx, because a cancelled invocation must still
// save or release its record, or the key stays blocked.
func cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
}

// release deletes the in-progress record so a retry can run again, and
// returns cause together with any delete error. If another invocation took
// over the key, its record is left alone.
func (i *Idempotency) release(ctx context.Context, rec Record, cause error) error {
	ctx, cancel := cleanupContext(ctx)
	defer cancel()

	err := i.store.Delete(ctx, rec)
	if err == nil || errors.Is(err, ErrNotOwner) {
		return cause
	}

	return errors.Join(cause, fmt.Errorf("idempotency: deleting record: %w", err))
}

func (i *Idempotency) key(data any) (string, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("idempotency: encoding key: %w", err)
	}
	if isEmptyJSON(b) {
		return "", ErrMissingKey
	}

	return i.prefix + "#" + hashBytes(b), nil
}

func replay[R any](rec Record, payloadHash string) (R, error) {
	var zero R

	// Records saved before validation was enabled have no hash; accept them.
	if payloadHash != "" && rec.PayloadHash != "" && payloadHash != rec.PayloadHash {
		return zero, ErrPayloadMismatch
	}

	switch rec.Status {
	case StatusInProgress:
		return zero, ErrInProgress
	case StatusCompleted:
		var result R
		if err := json.Unmarshal(rec.Response, &result); err != nil {
			return zero, fmt.Errorf("idempotency: decoding saved response: %w", err)
		}

		return result, nil
	default:
		return zero, fmt.Errorf("idempotency: unknown record status %q", rec.Status)
	}
}

// inProgressExpiry returns when an in-progress record may be taken over. In
// Lambda that is the invocation deadline, so a timed-out invocation does not
// block retries until the full expiry.
func inProgressExpiry(ctx context.Context, now time.Time, expiry time.Duration) time.Time {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline
	}

	return now.Add(expiry)
}

func hashJSON(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}

	return hashBytes(b), nil
}

func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)

	return hex.EncodeToString(sum[:])
}

func isEmptyJSON(b []byte) bool {
	b = bytes.TrimSpace(b)
	for _, empty := range []string{"null", `""`, "{}", "[]"} {
		if string(b) == empty {
			return true
		}
	}

	return false
}
