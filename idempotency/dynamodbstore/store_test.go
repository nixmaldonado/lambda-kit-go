package dynamodbstore

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/nixmaldonado/lambda-kit-go/idempotency"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	want := idempotency.Record{
		Key:                 "fn#abc",
		Status:              idempotency.StatusCompleted,
		ExpiresAt:           time.Unix(1_800_000_000, 0),
		InProgressExpiresAt: time.UnixMilli(1_800_000_000_123),
		Response:            []byte(`{"ok":true}`),
		PayloadHash:         "deadbeef",
		Token:               "tok",
	}

	got, err := decode(encode(want))
	if err != nil {
		t.Fatal(err)
	}

	sameTimes := got.ExpiresAt.Equal(want.ExpiresAt) && got.InProgressExpiresAt.Equal(want.InProgressExpiresAt)
	sameFields := got.Key == want.Key &&
		got.Status == want.Status &&
		string(got.Response) == string(want.Response) &&
		got.PayloadHash == want.PayloadHash &&
		got.Token == want.Token
	if !sameTimes || !sameFields {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
}

func TestDecodeOptionalAttributes(t *testing.T) {
	item := encode(idempotency.Record{
		Key:       "fn#abc",
		Status:    idempotency.StatusInProgress,
		ExpiresAt: time.Unix(1_800_000_000, 0),
	})

	got, err := decode(item)
	if err != nil {
		t.Fatal(err)
	}

	if !got.InProgressExpiresAt.IsZero() || got.Response != nil || got.PayloadHash != "" {
		t.Errorf("optional fields set: %+v", got)
	}
}

func TestEncodeRoundsExpiryUp(t *testing.T) {
	item := encode(idempotency.Record{
		Key:                 "k",
		ExpiresAt:           time.Unix(10, 100),
		InProgressExpiresAt: time.UnixMilli(10_000).Add(1),
	})

	if got := item[attrExpiry].(*types.AttributeValueMemberN).Value; got != "11" {
		t.Errorf("expiration = %s, want 11", got)
	}
	if got := item[attrInProgressExpiry].(*types.AttributeValueMemberN).Value; got != "10001" {
		t.Errorf("in_progress_expiration = %s, want 10001", got)
	}
}

// vanishingAPI fails the first conditional put without returning the old
// item, and then reports the item as gone, as if its owner deleted it in
// between.
type vanishingAPI struct {
	API
	puts int
}

func (a *vanishingAPI) PutItem(
	context.Context,
	*dynamodb.PutItemInput,
	...func(*dynamodb.Options),
) (*dynamodb.PutItemOutput, error) {
	a.puts++
	if a.puts == 1 {
		return nil, &types.ConditionalCheckFailedException{}
	}

	return &dynamodb.PutItemOutput{}, nil
}

func (a *vanishingAPI) GetItem(
	context.Context,
	*dynamodb.GetItemInput,
	...func(*dynamodb.Options),
) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}

func TestCreateRetriesWhenConflictVanishes(t *testing.T) {
	api := &vanishingAPI{}
	store := New(api, "t")
	now := time.Now()

	err := store.Create(context.Background(), idempotency.Record{Key: "k", ExpiresAt: now.Add(time.Hour)}, now)

	if err != nil {
		t.Errorf("Create = %v, want nil after retry", err)
	}
	if api.puts != 2 {
		t.Errorf("PutItem called %d times, want 2", api.puts)
	}
}

// The tests below run against a real DynamoDB endpoint, such as DynamoDB
// Local:
//
//	docker run --rm -p 8000:8000 amazon/dynamodb-local
//	LAMBDAKIT_DYNAMODB_ENDPOINT=http://localhost:8000 go test ./...

func newIntegrationStore(t *testing.T) *Store {
	t.Helper()

	endpoint := os.Getenv("LAMBDAKIT_DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.Skip("LAMBDAKIT_DYNAMODB_ENDPOINT not set")
	}

	client := dynamodb.New(dynamodb.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("local", "local", ""),
	})
	ctx := context.Background()
	table := "idempotency-" + strconv.FormatInt(time.Now().UnixNano(), 36)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(table),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrKey), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrKey), KeyType: types.KeyTypeHash},
		},
	})
	if err != nil {
		t.Fatalf("creating table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(table)})
	})

	return New(client, table)
}

func TestIntegrationCreateConflicts(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	now := time.Now()
	rec := idempotency.Record{
		Key:                 "fn#1",
		Status:              idempotency.StatusInProgress,
		ExpiresAt:           now.Add(time.Hour),
		InProgressExpiresAt: now.Add(time.Minute),
		PayloadHash:         "h1",
	}

	if err := store.Create(ctx, rec, now); err != nil {
		t.Fatalf("first create: %v", err)
	}
	err := store.Create(ctx, rec, now)

	var exists *idempotency.AlreadyExistsError
	if !errors.As(err, &exists) {
		t.Fatalf("second create err = %v, want *AlreadyExistsError", err)
	}
	if exists.Record.Status != idempotency.StatusInProgress || exists.Record.PayloadHash != "h1" {
		t.Errorf("existing record = %+v", exists.Record)
	}
}

func TestIntegrationCreateOverwritesStaleRecords(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	now := time.Now()

	tests := []struct {
		name string
		old  idempotency.Record
	}{
		{
			name: "expired",
			old: idempotency.Record{
				Status:    idempotency.StatusCompleted,
				ExpiresAt: now.Add(-time.Second),
				Response:  []byte(`1`),
			},
		},
		{
			name: "abandoned in progress",
			old: idempotency.Record{
				Status:              idempotency.StatusInProgress,
				ExpiresAt:           now.Add(time.Hour),
				InProgressExpiresAt: now.Add(-time.Second),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.old.Key = "fn#" + tt.name
			if err := store.Create(ctx, tt.old, now.Add(-2*time.Hour)); err != nil {
				t.Fatal(err)
			}
			fresh := idempotency.Record{
				Key:       tt.old.Key,
				Status:    idempotency.StatusInProgress,
				ExpiresAt: now.Add(time.Hour),
			}

			if err := store.Create(ctx, fresh, now); err != nil {
				t.Errorf("create over stale record: %v", err)
			}
		})
	}
}

func TestIntegrationHandler(t *testing.T) {
	store := newIntegrationStore(t)
	idem := idempotency.New(store, idempotency.WithKeyPrefix("it"))
	var calls atomic.Int32
	h := idempotency.Handler(idem, func(_ context.Context, id string) (string, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)

		return "done:" + id, nil
	})
	ctx := context.Background()

	const n = 8
	var wg sync.WaitGroup
	var completed, inProgress atomic.Int32
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h(ctx, "o-1")
			switch {
			case err == nil:
				completed.Add(1)
			case errors.Is(err, idempotency.ErrInProgress):
				inProgress.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if calls.Load() != 1 {
		t.Errorf("handler ran %d times under concurrency, want 1", calls.Load())
	}
	if completed.Load()+inProgress.Load() != n {
		t.Errorf("completed=%d inProgress=%d, want sum %d", completed.Load(), inProgress.Load(), n)
	}

	got, err := h(ctx, "o-1")
	if err != nil || got != "done:o-1" {
		t.Errorf("replay = %q, %v; want \"done:o-1\", nil", got, err)
	}

	errBoom := errors.New("boom")
	failing := idempotency.Handler(idem, func(context.Context, string) (string, error) {
		return "", errBoom
	})
	if _, err := failing(ctx, "o-2"); !errors.Is(err, errBoom) {
		t.Fatalf("failing handler err = %v", err)
	}
	if _, err := h(ctx, "o-2"); err != nil {
		t.Errorf("retry after failure: %v", err)
	}
}

func TestIntegrationOwnership(t *testing.T) {
	store := newIntegrationStore(t)
	ctx := context.Background()
	now := time.Now()
	mine := idempotency.Record{
		Key:       "fn#owned",
		Status:    idempotency.StatusInProgress,
		ExpiresAt: now.Add(time.Hour),
		Token:     "mine",
	}
	if err := store.Create(ctx, mine, now); err != nil {
		t.Fatal(err)
	}
	theirs := mine
	theirs.Token = "theirs"
	theirs.Status = idempotency.StatusCompleted

	if err := store.Update(ctx, theirs); !errors.Is(err, idempotency.ErrNotOwner) {
		t.Errorf("Update with other token = %v, want ErrNotOwner", err)
	}
	if err := store.Delete(ctx, theirs); !errors.Is(err, idempotency.ErrNotOwner) {
		t.Errorf("Delete with other token = %v, want ErrNotOwner", err)
	}
	if err := store.Update(ctx, mine); err != nil {
		t.Errorf("Update with own token: %v", err)
	}
	if err := store.Delete(ctx, mine); err != nil {
		t.Errorf("Delete with own token: %v", err)
	}
	if err := store.Delete(ctx, mine); err != nil {
		t.Errorf("Delete of missing record: %v", err)
	}
	if err := store.Update(ctx, mine); !errors.Is(err, idempotency.ErrNotOwner) {
		t.Errorf("Update of missing record = %v, want ErrNotOwner", err)
	}
}
