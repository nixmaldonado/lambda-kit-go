package idempotency

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Status is the state of an idempotency record.
type Status string

const (
	// StatusInProgress means an invocation is currently running for the key.
	StatusInProgress Status = "INPROGRESS"
	// StatusCompleted means an invocation finished and its response is saved.
	StatusCompleted Status = "COMPLETED"
)

// Record is what a [Store] persists for one idempotency key.
type Record struct {
	// Key is the idempotency key: the key prefix plus a hash of the key data.
	Key string
	// Status is the state of the invocation.
	Status Status
	// ExpiresAt is when the record stops protecting the key.
	ExpiresAt time.Time
	// InProgressExpiresAt is when an in-progress record is considered
	// abandoned (for example, after a Lambda timeout) and may be taken over.
	// Zero for completed records.
	InProgressExpiresAt time.Time
	// Response is the JSON-encoded result of a completed invocation.
	Response []byte
	// PayloadHash is the hash of the validated payload, or empty when payload
	// validation is not configured.
	PayloadHash string
	// Token identifies the invocation that created the record. Update and
	// Delete only act on a record whose token matches, so a slow invocation
	// cannot clobber the record of one that took over its key.
	Token string
}

// live reports whether r still blocks a new invocation with the same key.
func (r Record) live(now time.Time) bool {
	expired := !now.Before(r.ExpiresAt)
	abandoned := r.Status == StatusInProgress &&
		!r.InProgressExpiresAt.IsZero() &&
		!now.Before(r.InProgressExpiresAt)

	return !expired && !abandoned
}

// Store persists idempotency records. Implementations must be safe for
// concurrent use.
type Store interface {
	// Create saves r unless a live record with the same key already exists.
	//
	// A record is live when now is before its ExpiresAt and, for an
	// in-progress record, before its InProgressExpiresAt. Expired and
	// abandoned records must be overwritten.
	//
	// The check and the write must be atomic. When a live record exists,
	// Create returns an *[AlreadyExistsError] holding that record.
	Create(ctx context.Context, r Record, now time.Time) error

	// Update overwrites the record with r.Key, but only if the stored record
	// has the same Token. Otherwise, including when no record exists, it
	// returns [ErrNotOwner].
	Update(ctx context.Context, r Record) error

	// Delete removes the record with r.Key, but only if the stored record has
	// the same Token. Deleting a missing record is not an error; a record with
	// another token returns [ErrNotOwner].
	Delete(ctx context.Context, r Record) error
}

// AlreadyExistsError is returned by [Store.Create] when a live record exists.
type AlreadyExistsError struct {
	Record Record
}

func (e *AlreadyExistsError) Error() string {
	return fmt.Sprintf("idempotency: record %q already exists", e.Record.Key)
}

var (
	// ErrInProgress is returned when another invocation with the same key is
	// still running.
	ErrInProgress = errors.New("idempotency: invocation already in progress")

	// ErrPayloadMismatch is returned when a request reuses a key with a
	// different payload than the one saved, when payload validation is on.
	ErrPayloadMismatch = errors.New("idempotency: payload does not match saved record")

	// ErrMissingKey is returned when the key data is empty: JSON null, "",
	// {} or [].
	ErrMissingKey = errors.New("idempotency: key data is empty")

	// ErrResponseNotSaved is returned, together with the result, when the
	// function succeeded but its response could not be saved. The side effect
	// has happened. Returning this error to Lambda makes it retry, and the
	// retry runs the function again once the in-progress record expires; a
	// caller that prefers no retry can check for it and return a nil error.
	ErrResponseNotSaved = errors.New("idempotency: function succeeded but its response was not saved")

	// ErrNotOwner is returned by [Store.Update] and [Store.Delete] when the
	// record belongs to another invocation.
	ErrNotOwner = errors.New("idempotency: record is owned by another invocation")
)
