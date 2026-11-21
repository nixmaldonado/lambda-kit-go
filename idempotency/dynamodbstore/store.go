// Package dynamodbstore implements [idempotency.Store] on Amazon DynamoDB.
//
// The table needs a string partition key named "id". Enable DynamoDB TTL on
// the "expiration" attribute so expired records are removed. The attribute
// names match Powertools for AWS Lambda, so an existing Powertools
// idempotency table can be reused.
//
// Each record also stores the creating invocation's token in "lk_token", so a
// slow invocation cannot overwrite or delete a record another invocation took
// over.
//
// The Lambda role needs dynamodb:PutItem, dynamodb:GetItem and
// dynamodb:DeleteItem on the table.
package dynamodbstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/nixmaldonado/lambda-kit-go/idempotency"
)

const (
	attrKey              = "id"
	attrExpiry           = "expiration"
	attrInProgressExpiry = "in_progress_expiration"
	attrStatus           = "status"
	attrResponse         = "data"
	attrPayloadHash      = "validation"
	attrToken            = "lk_token"
)

// createAttempts bounds retries when a conflicting record disappears between
// the failed write and the read that follows it.
const createAttempts = 3

// ownerCondition matches a record written by the same invocation.
const ownerCondition = "#token = :token"

// createCondition lets a write through when no live record exists: none at
// all, an expired one, or an in-progress one whose invocation was abandoned.
const createCondition = "attribute_not_exists(#id)" +
	" OR #exp <= :now" +
	" OR (#status = :inprogress AND #ipexp <= :nowms)"

// API is the subset of the DynamoDB client the store uses. *dynamodb.Client
// satisfies it.
type API interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// Store is an [idempotency.Store] backed by a DynamoDB table.
type Store struct {
	client API
	table  string
}

var _ idempotency.Store = (*Store)(nil)

// New returns a Store that saves records in the given table.
func New(client API, table string) *Store {
	return &Store{client: client, table: table}
}

// Create implements [idempotency.Store].
func (s *Store) Create(ctx context.Context, r idempotency.Record, now time.Time) error {
	for range createAttempts {
		err := s.tryCreate(ctx, r, now)
		if !errors.Is(err, errVanished) {
			return err
		}
	}

	return fmt.Errorf("dynamodbstore: record %q kept changing during create", r.Key)
}

// errVanished means the record that blocked a create was gone by the time it
// was read, so the create can be tried again.
var errVanished = errors.New("dynamodbstore: conflicting record vanished")

func (s *Store) tryCreate(ctx context.Context, r idempotency.Record, now time.Time) error {
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(s.table),
		Item:                encode(r),
		ConditionExpression: aws.String(createCondition),
		ExpressionAttributeNames: map[string]string{
			"#id":     attrKey,
			"#exp":    attrExpiry,
			"#status": attrStatus,
			"#ipexp":  attrInProgressExpiry,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":now":        number(now.Unix()),
			":nowms":      number(now.UnixMilli()),
			":inprogress": &types.AttributeValueMemberS{Value: string(idempotency.StatusInProgress)},
		},
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})

	var failed *types.ConditionalCheckFailedException
	switch {
	case errors.As(err, &failed):
		return s.existing(ctx, r.Key, failed.Item)
	case err != nil:
		return fmt.Errorf("dynamodbstore: put item: %w", err)
	}

	return nil
}

// existing builds the AlreadyExistsError for a failed create. It reads the
// record when DynamoDB did not return it with the failure.
func (s *Store) existing(ctx context.Context, key string, item map[string]types.AttributeValue) error {
	if len(item) == 0 {
		out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(s.table),
			Key:            keyOf(key),
			ConsistentRead: aws.Bool(true),
		})
		if err != nil {
			return fmt.Errorf("dynamodbstore: get item: %w", err)
		}
		item = out.Item
	}
	if len(item) == 0 {
		return errVanished
	}

	rec, err := decode(item)
	if err != nil {
		return err
	}

	return &idempotency.AlreadyExistsError{Record: rec}
}

// Update implements [idempotency.Store].
func (s *Store) Update(ctx context.Context, r idempotency.Record) error {
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(s.table),
		Item:                      encode(r),
		ConditionExpression:       aws.String(ownerCondition),
		ExpressionAttributeNames:  map[string]string{"#token": attrToken},
		ExpressionAttributeValues: tokenValue(r.Token),
	})

	var failed *types.ConditionalCheckFailedException
	switch {
	case errors.As(err, &failed):
		return idempotency.ErrNotOwner
	case err != nil:
		return fmt.Errorf("dynamodbstore: put item: %w", err)
	}

	return nil
}

// Delete implements [idempotency.Store].
func (s *Store) Delete(ctx context.Context, r idempotency.Record) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:           aws.String(s.table),
		Key:                 keyOf(r.Key),
		ConditionExpression: aws.String("attribute_not_exists(#id) OR " + ownerCondition),
		ExpressionAttributeNames: map[string]string{
			"#id":    attrKey,
			"#token": attrToken,
		},
		ExpressionAttributeValues: tokenValue(r.Token),
	})

	var failed *types.ConditionalCheckFailedException
	switch {
	case errors.As(err, &failed):
		return idempotency.ErrNotOwner
	case err != nil:
		return fmt.Errorf("dynamodbstore: delete item: %w", err)
	}

	return nil
}

func tokenValue(token string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		":token": &types.AttributeValueMemberS{Value: token},
	}
}

func keyOf(key string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrKey: &types.AttributeValueMemberS{Value: key},
	}
}

func encode(r idempotency.Record) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		attrKey:    &types.AttributeValueMemberS{Value: r.Key},
		attrStatus: &types.AttributeValueMemberS{Value: string(r.Status)},
		attrExpiry: number(ceilUnix(r.ExpiresAt)),
	}
	if !r.InProgressExpiresAt.IsZero() {
		item[attrInProgressExpiry] = number(ceilUnixMilli(r.InProgressExpiresAt))
	}
	if r.Token != "" {
		item[attrToken] = &types.AttributeValueMemberS{Value: r.Token}
	}
	if r.Response != nil {
		item[attrResponse] = &types.AttributeValueMemberS{Value: string(r.Response)}
	}
	if r.PayloadHash != "" {
		item[attrPayloadHash] = &types.AttributeValueMemberS{Value: r.PayloadHash}
	}

	return item
}

func decode(item map[string]types.AttributeValue) (idempotency.Record, error) {
	var r idempotency.Record
	if len(item) == 0 {
		return r, errors.New("dynamodbstore: record not found")
	}

	key, ok := str(item, attrKey)
	if !ok {
		return r, fmt.Errorf("dynamodbstore: record has no %q attribute", attrKey)
	}
	status, _ := str(item, attrStatus)
	r.Key = key
	r.Status = idempotency.Status(status)

	exp, err := num(item, attrExpiry)
	if err != nil {
		return r, err
	}
	r.ExpiresAt = time.Unix(exp, 0)

	if _, ok := item[attrInProgressExpiry]; ok {
		ms, err := num(item, attrInProgressExpiry)
		if err != nil {
			return r, err
		}
		r.InProgressExpiresAt = time.UnixMilli(ms)
	}
	if data, ok := str(item, attrResponse); ok {
		r.Response = []byte(data)
	}
	r.PayloadHash, _ = str(item, attrPayloadHash)
	r.Token, _ = str(item, attrToken)

	return r, nil
}

// ceilUnix rounds up to whole seconds, so a stored record never expires
// earlier than the in-memory one it came from.
func ceilUnix(t time.Time) int64 {
	secs := t.Unix()
	if t.Nanosecond() > 0 {
		secs++
	}

	return secs
}

// ceilUnixMilli rounds up to whole milliseconds, for the same reason.
func ceilUnixMilli(t time.Time) int64 {
	ms := t.UnixMilli()
	if t.Nanosecond()%int(time.Millisecond) > 0 {
		ms++
	}

	return ms
}

func number(n int64) *types.AttributeValueMemberN {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(n, 10)}
}

func str(item map[string]types.AttributeValue, name string) (string, bool) {
	v, ok := item[name].(*types.AttributeValueMemberS)
	if !ok {
		return "", false
	}

	return v.Value, true
}

func num(item map[string]types.AttributeValue, name string) (int64, error) {
	v, ok := item[name].(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("dynamodbstore: record has no numeric %q attribute", name)
	}
	n, err := strconv.ParseInt(v.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("dynamodbstore: parsing %q: %w", name, err)
	}

	return n, nil
}
