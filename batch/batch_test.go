package batch

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

const (
	standardARN = "arn:aws:sqs:us-east-1:123456789012:orders"
	fifoARN     = "arn:aws:sqs:us-east-1:123456789012:orders.fifo"
)

var errBoom = errors.New("boom")

func sqsEvent(arn string, ids ...string) events.SQSEvent {
	ev := events.SQSEvent{Records: []events.SQSMessage{}}
	for _, id := range ids {
		ev.Records = append(ev.Records, events.SQSMessage{MessageId: id, Body: id, EventSourceARN: arn})
	}

	return ev
}

// failOn returns a record function that fails for the given bodies and
// records which bodies it saw.
func failOn(seen *[]string, bad ...string) func(context.Context, events.SQSMessage) error {
	return func(_ context.Context, m events.SQSMessage) error {
		*seen = append(*seen, m.Body)
		if slices.Contains(bad, m.Body) {
			return errBoom
		}

		return nil
	}
}

func sqsFailures(resp events.SQSEventResponse) []string {
	ids := []string{}
	for _, f := range resp.BatchItemFailures {
		ids = append(ids, f.ItemIdentifier)
	}

	return ids
}

func TestSQS(t *testing.T) {
	tests := []struct {
		name       string
		arn        string
		bad        []string
		wantSeen   []string
		wantFailed []string
		wantAll    bool
	}{
		{
			name:       "all succeed",
			arn:        standardARN,
			wantSeen:   []string{"a", "b", "c"},
			wantFailed: []string{},
		},
		{
			name:       "standard reports each failure and keeps going",
			arn:        standardARN,
			bad:        []string{"a", "c"},
			wantSeen:   []string{"a", "b", "c"},
			wantFailed: []string{"a", "c"},
		},
		{
			name:       "standard all fail",
			arn:        standardARN,
			bad:        []string{"a", "b", "c"},
			wantSeen:   []string{"a", "b", "c"},
			wantFailed: []string{"a", "b", "c"},
			wantAll:    true,
		},
		{
			name:       "fifo stops and reports the rest",
			arn:        fifoARN,
			bad:        []string{"b"},
			wantSeen:   []string{"a", "b"},
			wantFailed: []string{"b", "c"},
		},
		{
			name:       "fifo first fails",
			arn:        fifoARN,
			bad:        []string{"a"},
			wantSeen:   []string{"a"},
			wantFailed: []string{"a", "b", "c"},
			wantAll:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := []string{}
			h := SQS(failOn(&seen, tt.bad...))

			resp, err := h(context.Background(), sqsEvent(tt.arn, "a", "b", "c"))

			if got := errors.Is(err, ErrAllFailed); got != tt.wantAll {
				t.Errorf("err = %v, want ErrAllFailed: %v", err, tt.wantAll)
			}
			if err != nil && !errors.Is(err, errBoom) {
				t.Errorf("err = %v, want it to wrap the record error", err)
			}
			if !slices.Equal(seen, tt.wantSeen) {
				t.Errorf("processed %v, want %v", seen, tt.wantSeen)
			}
			if got := sqsFailures(resp); !slices.Equal(got, tt.wantFailed) {
				t.Errorf("reported %v, want %v", got, tt.wantFailed)
			}
		})
	}
}

func TestSQSEmptyBatch(t *testing.T) {
	resp, err := SQS(func(context.Context, events.SQSMessage) error { return nil })(
		context.Background(),
		events.SQSEvent{},
	)

	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Errorf("empty batch = %+v, %v; want no failures, nil", resp, err)
	}
}

func TestPanicFailsOnlyItsRecord(t *testing.T) {
	h := SQS(func(_ context.Context, m events.SQSMessage) error {
		if m.Body == "b" {
			panic("bad record")
		}

		return nil
	})

	resp, err := h(context.Background(), sqsEvent(standardARN, "a", "b", "c"))

	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if got := sqsFailures(resp); !slices.Equal(got, []string{"b"}) {
		t.Errorf("reported %v, want [b]", got)
	}
}

func TestOnErrorReceivesFailures(t *testing.T) {
	type failure struct {
		id  string
		err error
	}
	got := []failure{}
	seen := []string{}
	h := SQS(
		failOn(&seen, "b"),
		OnError(func(_ context.Context, id string, err error) {
			got = append(got, failure{id: id, err: err})
		}),
	)

	if _, err := h(context.Background(), sqsEvent(standardARN, "a", "b")); err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 || got[0].id != "b" || !errors.Is(got[0].err, errBoom) {
		t.Errorf("OnError calls = %+v, want one for b with errBoom", got)
	}
}

func TestCancelledContextReportsUnprocessed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	seen := []string{}
	h := SQS(func(_ context.Context, m events.SQSMessage) error {
		seen = append(seen, m.Body)
		cancel() // the invocation runs out of time after the first record

		return nil
	})

	resp, err := h(ctx, sqsEvent(standardARN, "a", "b", "c"))

	if err != nil {
		t.Errorf("err = %v, want nil since a succeeded", err)
	}
	if !slices.Equal(seen, []string{"a"}) {
		t.Errorf("processed %v, want [a]", seen)
	}
	if got := sqsFailures(resp); !slices.Equal(got, []string{"b", "c"}) {
		t.Errorf("reported %v, want [b c]", got)
	}
}

func TestKinesisStopsAndReportsFirstFailure(t *testing.T) {
	seen := []string{}
	h := Kinesis(func(_ context.Context, r events.KinesisEventRecord) error {
		seen = append(seen, r.Kinesis.SequenceNumber)
		if r.Kinesis.SequenceNumber == "2" {
			return errBoom
		}

		return nil
	})
	ev := events.KinesisEvent{Records: []events.KinesisEventRecord{}}
	for _, seq := range []string{"1", "2", "3"} {
		ev.Records = append(ev.Records, events.KinesisEventRecord{
			Kinesis: events.KinesisRecord{SequenceNumber: seq},
		})
	}

	resp, err := h(context.Background(), ev)

	if err != nil {
		t.Errorf("err = %v, want nil since 1 succeeded", err)
	}
	if !slices.Equal(seen, []string{"1", "2"}) {
		t.Errorf("processed %v, want [1 2]", seen)
	}
	if len(resp.BatchItemFailures) != 1 || resp.BatchItemFailures[0].ItemIdentifier != "2" {
		t.Errorf("reported %+v, want only 2", resp.BatchItemFailures)
	}
}

func TestDynamoDBStopsAndReportsFirstFailure(t *testing.T) {
	seen := []string{}
	h := DynamoDB(func(_ context.Context, r events.DynamoDBEventRecord) error {
		seen = append(seen, r.Change.SequenceNumber)

		return errBoom
	})
	ev := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		{Change: events.DynamoDBStreamRecord{SequenceNumber: "10"}},
		{Change: events.DynamoDBStreamRecord{SequenceNumber: "11"}},
	}}

	resp, err := h(context.Background(), ev)

	if !errors.Is(err, ErrAllFailed) {
		t.Errorf("err = %v, want ErrAllFailed", err)
	}
	if !slices.Equal(seen, []string{"10"}) {
		t.Errorf("processed %v, want [10]", seen)
	}
	if len(resp.BatchItemFailures) != 1 || resp.BatchItemFailures[0].ItemIdentifier != "10" {
		t.Errorf("reported %+v, want only 10", resp.BatchItemFailures)
	}
}
