// Package e2e holds what the loadgen and verify commands share.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Outputs are the Terraform outputs the commands need.
type Outputs struct {
	Region           string
	QueueURL         string
	DLQURL           string
	IdempotencyTable string
	LedgerTable      string
	LogGroup         string
	ProjectTag       string
}

// LoadOutputs reads the file written by `terraform output -json`.
func LoadOutputs(path string) (Outputs, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Outputs{}, fmt.Errorf("reading terraform outputs: %w", err)
	}

	var raw map[string]struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return Outputs{}, fmt.Errorf("parsing terraform outputs: %w", err)
	}

	out := Outputs{
		Region:           raw["region"].Value,
		QueueURL:         raw["queue_url"].Value,
		DLQURL:           raw["dlq_url"].Value,
		IdempotencyTable: raw["idempotency_table"].Value,
		LedgerTable:      raw["ledger_table"].Value,
		LogGroup:         raw["log_group"].Value,
		ProjectTag:       raw["project_tag"].Value,
	}
	if out.Region == "" || out.QueueURL == "" || out.DLQURL == "" || out.LedgerTable == "" {
		return Outputs{}, fmt.Errorf("terraform outputs in %s are incomplete; run `make deploy` first", path)
	}

	return out, nil
}

// Manifest records what loadgen sent.
type Manifest struct {
	// Run prefixes every order ID in this run.
	Run string `json:"run"`
	// Orders are the valid order IDs; each must end up charged exactly once.
	Orders []string `json:"orders"`
	// Messages is how many messages were sent, duplicates and poison included.
	Messages int `json:"messages"`
	// Poison is how many invalid messages were sent.
	Poison int       `json:"poison"`
	SentAt time.Time `json:"sentAt"`
}

// PoisonPrefix starts every invalid message body, followed by the run ID.
const PoisonPrefix = "not json {"

// WriteJSON writes v to path as indented JSON.
func WriteJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, b, 0o644)
}

// QueueDepth returns the visible plus in-flight messages in a queue. SQS
// reports approximate numbers that can lag by up to a minute.
func QueueDepth(ctx context.Context, client *sqs.Client, queueURL string) (int, error) {
	res, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		},
	})
	if err != nil {
		return 0, fmt.Errorf("reading queue attributes: %w", err)
	}

	total := 0
	for _, v := range res.Attributes {
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("parsing queue attribute %q: %w", v, err)
		}
		total += n
	}

	return total, nil
}
