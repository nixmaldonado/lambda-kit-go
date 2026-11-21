package batch

import (
	"context"
	"strings"

	"github.com/aws/aws-lambda-go/events"
)

// SQS returns a Lambda handler for SQS events that calls process once per
// message and reports the failed ones. FIFO queues are detected from the
// queue ARN and keep their order: see the package documentation.
func SQS(
	process func(context.Context, events.SQSMessage) error,
	opts ...Option,
) func(context.Context, events.SQSEvent) (events.SQSEventResponse, error) {
	cfg := newConfig(opts)

	return func(ctx context.Context, ev events.SQSEvent) (events.SQSEventResponse, error) {
		m := continueOnFailure
		if isFIFO(ev.Records) {
			m = stopReportRest
		}

		failed, err := run(ctx, ev.Records, sqsID, process, m, cfg)

		resp := events.SQSEventResponse{
			BatchItemFailures: make([]events.SQSBatchItemFailure, 0, len(failed)),
		}
		for _, id := range failed {
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: id})
		}

		return resp, err
	}
}

// Kinesis returns a Lambda handler for Kinesis events that calls process once
// per record, stops at the first failure and reports it.
func Kinesis(
	process func(context.Context, events.KinesisEventRecord) error,
	opts ...Option,
) func(context.Context, events.KinesisEvent) (events.KinesisEventResponse, error) {
	cfg := newConfig(opts)

	return func(ctx context.Context, ev events.KinesisEvent) (events.KinesisEventResponse, error) {
		failed, err := run(ctx, ev.Records, kinesisID, process, stopReportFirst, cfg)

		resp := events.KinesisEventResponse{
			BatchItemFailures: make([]events.KinesisBatchItemFailure, 0, len(failed)),
		}
		for _, id := range failed {
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.KinesisBatchItemFailure{ItemIdentifier: id})
		}

		return resp, err
	}
}

// DynamoDB returns a Lambda handler for DynamoDB Streams events that calls
// process once per record, stops at the first failure and reports it.
func DynamoDB(
	process func(context.Context, events.DynamoDBEventRecord) error,
	opts ...Option,
) func(context.Context, events.DynamoDBEvent) (events.DynamoDBEventResponse, error) {
	cfg := newConfig(opts)

	return func(ctx context.Context, ev events.DynamoDBEvent) (events.DynamoDBEventResponse, error) {
		failed, err := run(ctx, ev.Records, dynamoDBID, process, stopReportFirst, cfg)

		resp := events.DynamoDBEventResponse{
			BatchItemFailures: make([]events.DynamoDBBatchItemFailure, 0, len(failed)),
		}
		for _, id := range failed {
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.DynamoDBBatchItemFailure{ItemIdentifier: id})
		}

		return resp, err
	}
}

func sqsID(m events.SQSMessage) string { return m.MessageId }

func kinesisID(r events.KinesisEventRecord) string { return r.Kinesis.SequenceNumber }

func dynamoDBID(r events.DynamoDBEventRecord) string { return r.Change.SequenceNumber }

// isFIFO reports whether the messages come from a FIFO queue, whose ARNs end
// in ".fifo". All records in one event come from the same queue.
func isFIFO(records []events.SQSMessage) bool {
	return len(records) > 0 && strings.HasSuffix(records[0].EventSourceARN, ".fifo")
}
