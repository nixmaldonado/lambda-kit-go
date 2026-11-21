// Command verify checks a loadgen run: it waits for the queue to drain, then
// checks that every order was charged exactly once, that only invalid
// messages reached the DLQ, and counts the log lines that prove the
// interesting paths ran.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/nixmaldonado/lambda-kit-go/examples/sqs-orders/e2e/internal/e2e"
)

func main() {
	outputsPath := flag.String("outputs", "build/outputs.json", "terraform output -json file")
	manifestPath := flag.String("manifest", "build/manifest.json", "manifest written by loadgen")
	timeout := flag.Duration("timeout", 15*time.Minute, "how long to wait for the queue to drain")
	flag.Parse()

	ctx := context.Background()
	out, err := e2e.LoadOutputs(*outputsPath)
	if err != nil {
		log.Fatal(err)
	}
	m, err := loadManifest(*manifestPath)
	if err != nil {
		log.Fatal(err)
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(out.Region))
	if err != nil {
		log.Fatal(err)
	}
	sqsClient := sqs.NewFromConfig(cfg)

	if err := waitForDrain(ctx, sqsClient, out.QueueURL, *timeout); err != nil {
		log.Fatal(err)
	}

	charges, err := scanLedger(ctx, dynamodb.NewFromConfig(cfg), out.LedgerTable, m.Run)
	if err != nil {
		log.Fatal(err)
	}
	dlq, err := readDLQ(ctx, sqsClient, out.DLQURL, m.Run)
	if err != nil {
		log.Fatal(err)
	}
	logs := cloudwatchlogs.NewFromConfig(cfg)
	counts := map[string]int{}
	for _, phrase := range []string{
		"already in progress",      // ErrInProgress: a concurrent duplicate was blocked
		"record not processed",     // ErrNotProcessed: the timeout margin handed records back
		"AccessDenied",             // a missing IAM permission
		"charged but not recorded", // ErrResponseNotSaved
		"not enough time left",     // the example's headroom guard deferred a charge
		"Task timed out",           // a hard timeout, older log format
		"Status: timeout",          // a hard timeout, REPORT line format
	} {
		if counts[phrase], err = countLogLines(ctx, logs, out.LogGroup, phrase, m.SentAt); err != nil {
			log.Fatal(err)
		}
	}

	if !report(m, charges, dlq, counts) {
		os.Exit(1)
	}
}

func loadManifest(path string) (e2e.Manifest, error) {
	var m e2e.Manifest
	b, err := os.ReadFile(path)
	if err != nil {
		return m, fmt.Errorf("reading manifest: %w", err)
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("parsing manifest: %w", err)
	}

	return m, nil
}

// waitForDrain waits until the main queue is empty three polls in a row,
// since SQS counts are approximate. Once it is, every message was either
// processed or moved to the DLQ.
func waitForDrain(ctx context.Context, client *sqs.Client, queueURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	stable := 0

	for stable < 3 {
		if time.Now().After(deadline) {
			return fmt.Errorf("queue did not drain within %v", timeout)
		}

		depth, err := e2e.QueueDepth(ctx, client, queueURL)
		if err != nil {
			return err
		}
		log.Printf("queue: %d visible or in flight", depth)

		if depth == 0 {
			stable++
		} else {
			stable = 0
		}
		time.Sleep(10 * time.Second)
	}

	return nil
}

// scanLedger returns the charge count per order for this run.
func scanLedger(ctx context.Context, client *dynamodb.Client, table, run string) (map[string]int, error) {
	charges := map[string]int{}
	p := dynamodb.NewScanPaginator(client, &dynamodb.ScanInput{
		TableName:                 aws.String(table),
		FilterExpression:          aws.String("begins_with(order_id, :run)"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":run": &ddbtypes.AttributeValueMemberS{Value: run + "-"}},
		ConsistentRead:            aws.Bool(true),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("scanning ledger: %w", err)
		}
		for _, item := range page.Items {
			id, _ := item["order_id"].(*ddbtypes.AttributeValueMemberS)
			n, _ := item["charges"].(*ddbtypes.AttributeValueMemberN)
			if id == nil || n == nil {
				continue
			}
			count, err := strconv.Atoi(n.Value)
			if err != nil {
				return nil, fmt.Errorf("parsing charges for %s: %w", id.Value, err)
			}
			charges[id.Value] = count
		}
	}

	return charges, nil
}

// dlqContents is what this run left in the DLQ.
type dlqContents struct {
	poison int      // this run's invalid messages
	valid  []string // this run's valid orders, which should be none
}

// readDLQ reads the whole DLQ without deleting anything and picks out this
// run's messages. Each message read is hidden for 10 minutes, so it is not
// counted twice; it stops after several empty receives in a row.
func readDLQ(ctx context.Context, client *sqs.Client, dlqURL, run string) (dlqContents, error) {
	c := dlqContents{valid: []string{}}
	poisonPrefix := e2e.PoisonPrefix + run + "-"

	for empty := 0; empty < 3; {
		res, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(dlqURL),
			MaxNumberOfMessages: 10,
			VisibilityTimeout:   600,
			WaitTimeSeconds:     5,
		})
		if err != nil {
			return c, fmt.Errorf("reading DLQ: %w", err)
		}
		if len(res.Messages) == 0 {
			empty++

			continue
		}
		empty = 0

		for _, msg := range res.Messages {
			body := aws.ToString(msg.Body)
			if strings.HasPrefix(body, poisonPrefix) {
				c.poison++

				continue
			}
			var o struct {
				ID string `json:"id"`
			}
			if json.Unmarshal([]byte(body), &o) == nil && strings.HasPrefix(o.ID, run+"-") {
				c.valid = append(c.valid, o.ID)
			}
		}
	}

	return c, nil
}

func countLogLines(ctx context.Context, client *cloudwatchlogs.Client, group, phrase string, since time.Time) (int, error) {
	count := 0
	p := cloudwatchlogs.NewFilterLogEventsPaginator(client, &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName:  aws.String(group),
		FilterPattern: aws.String(strconv.Quote(phrase)),
		StartTime:     aws.Int64(since.Add(-time.Minute).UnixMilli()),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("searching logs for %q: %w", phrase, err)
		}
		count += len(page.Events)
	}

	return count, nil
}

// report prints the results and returns whether the run passed.
func report(m e2e.Manifest, charges map[string]int, dlq dlqContents, counts map[string]int) bool {
	missing, doubled := []string{}, []string{}
	for _, id := range m.Orders {
		switch n := charges[id]; {
		case n == 0:
			missing = append(missing, id)
		case n > 1:
			doubled = append(doubled, fmt.Sprintf("%s (x%d)", id, n))
		}
	}

	fmt.Printf("\nRun %s: %d messages for %d orders, %d poison\n\n", m.Run, m.Messages, len(m.Orders), m.Poison)
	pass := true
	check := func(ok bool, format string, args ...any) {
		mark := "PASS"
		if !ok {
			mark, pass = "FAIL", false
		}
		fmt.Printf("  [%s] %s\n", mark, fmt.Sprintf(format, args...))
	}

	check(len(doubled) == 0, "no order charged twice (%d doubled) %s", len(doubled), sample(doubled))
	check(len(missing) == 0, "every order charged (%d missing) %s", len(missing), sample(missing))
	check(len(dlq.valid) == 0, "no valid order in the DLQ (%d found) %s", len(dlq.valid), sample(dlq.valid))
	check(dlq.poison == m.Poison, "every invalid message reached the DLQ (%d of %d)", dlq.poison, m.Poison)
	check(counts["AccessDenied"] == 0, "IAM permissions sufficient (%d AccessDenied lines)", counts["AccessDenied"])

	fmt.Println("\n  Evidence the interesting paths ran:")
	fmt.Printf("    concurrent duplicates blocked (ErrInProgress):  %d\n", counts["already in progress"])
	fmt.Printf("    charges deferred by the headroom guard:         %d\n", counts["not enough time left"])
	fmt.Printf("    records handed back near timeout:               %d\n", counts["record not processed"])
	fmt.Printf("    invocations that hit the hard timeout:          %d\n", counts["Task timed out"]+counts["Status: timeout"])
	fmt.Printf("    charged but not recorded:                       %d\n", counts["charged but not recorded"])
	fmt.Println()

	return pass
}

func sample(ids []string) string {
	if len(ids) == 0 {
		return ""
	}

	return fmt.Sprintf("e.g. %v", ids[:min(len(ids), 5)])
}

func init() {
	log.SetFlags(log.Ltime)
}
