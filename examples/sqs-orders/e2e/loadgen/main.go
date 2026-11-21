// Command loadgen sends realistic order traffic to the example's queue,
// including the cases idempotency and batching have to handle: producer
// duplicates, duplicates sent at the same instant, invalid messages and slow
// orders. It writes a manifest that the verify command checks afterwards.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/nixmaldonado/lambda-kit-go/examples/sqs-orders/e2e/internal/e2e"
)

type order struct {
	ID      string `json:"id"`
	Amount  int    `json:"amount"`
	DelayMs int    `json:"delayMs,omitempty"`
}

func main() {
	outputsPath := flag.String("outputs", "build/outputs.json", "terraform output -json file")
	manifestPath := flag.String("manifest", "build/manifest.json", "where to write the manifest")
	orders := flag.Int("orders", 1000, "distinct orders sent once each")
	dupOrders := flag.Int("dup-orders", 200, "orders a producer publishes 2-4 times, spread through the traffic")
	raceOrders := flag.Int("race-orders", 50, "orders sent 5 times at the same instant")
	poison := flag.Int("poison", 20, "invalid messages that must end up in the DLQ")
	slow := flag.Int("slow", 5, "orders that sleep close to the function timeout")
	slowMs := flag.Int("slow-ms", 6000, "how long slow orders sleep; with 2s of headroom they need 8s of a 10s timeout")
	flag.Parse()

	ctx := context.Background()
	out, err := e2e.LoadOutputs(*outputsPath)
	if err != nil {
		log.Fatal(err)
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(out.Region))
	if err != nil {
		log.Fatal(err)
	}
	client := sqs.NewFromConfig(cfg)

	run := "r" + strconv.FormatInt(time.Now().Unix(), 36)
	m := e2e.Manifest{Run: run, Poison: *poison, SentAt: time.Now()}
	bodies := []string{}
	n := 0
	newOrder := func(delayMs int) order {
		n++

		return order{ID: fmt.Sprintf("%s-o%05d", run, n), Amount: 1 + rand.IntN(500), DelayMs: delayMs}
	}
	add := func(o order, copies int) {
		b, err := json.Marshal(o)
		if err != nil {
			log.Fatal(err)
		}
		for range copies {
			bodies = append(bodies, string(b))
		}
		m.Orders = append(m.Orders, o.ID)
	}

	for range *orders {
		add(newOrder(0), 1)
	}
	for range *dupOrders {
		add(newOrder(0), 2+rand.IntN(3))
	}
	for range *slow {
		// Each slow order is duplicated too, so a copy hits ErrInProgress
		// while the first one is still sleeping.
		add(newOrder(*slowMs), 2)
	}
	for i := range *poison {
		// The run ID lets verify count this run's poison in the DLQ.
		bodies = append(bodies, fmt.Sprintf("%s%s-%d", e2e.PoisonPrefix, run, i))
	}
	rand.Shuffle(len(bodies), func(i, j int) { bodies[i], bodies[j] = bodies[j], bodies[i] })

	log.Printf("run %s: sending %d messages", run, len(bodies))
	if err := sendBatches(ctx, client, out.QueueURL, bodies); err != nil {
		log.Fatal(err)
	}
	m.Messages = len(bodies)

	// Race phase: each order is sent 5 times by 5 goroutines released
	// together, so copies land in different batches and different
	// Lambda instances at the same moment.
	log.Printf("run %s: sending %d race orders x5", run, *raceOrders)
	for range *raceOrders {
		o := newOrder(0)
		b, err := json.Marshal(o)
		if err != nil {
			log.Fatal(err)
		}
		if err := sendTogether(ctx, client, out.QueueURL, string(b), 5); err != nil {
			log.Fatal(err)
		}
		m.Orders = append(m.Orders, o.ID)
		m.Messages += 5
	}

	if err := e2e.WriteJSON(*manifestPath, m); err != nil {
		log.Fatal(err)
	}
	log.Printf("run %s: sent %d messages for %d orders (+%d poison); manifest at %s",
		run, m.Messages, len(m.Orders), *poison, *manifestPath)
}

// sendBatches sends bodies 10 at a time from a few workers.
func sendBatches(ctx context.Context, client *sqs.Client, queueURL string, bodies []string) error {
	chunks := make(chan []string)
	errs := make(chan error, 1)
	var wg sync.WaitGroup

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range chunks {
				if err := sendBatch(ctx, client, queueURL, chunk); err != nil {
					select {
					case errs <- err:
					default:
					}
				}
			}
		}()
	}

	for start := 0; start < len(bodies); start += 10 {
		chunks <- bodies[start:min(start+10, len(bodies))]
	}
	close(chunks)
	wg.Wait()

	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func sendBatch(ctx context.Context, client *sqs.Client, queueURL string, bodies []string) error {
	entries := make([]types.SendMessageBatchRequestEntry, 0, len(bodies))
	for i, body := range bodies {
		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id:          aws.String(strconv.Itoa(i)),
			MessageBody: aws.String(body),
		})
	}

	res, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(queueURL), Entries: entries})
	if err != nil {
		return fmt.Errorf("sending batch: %w", err)
	}
	if len(res.Failed) > 0 {
		return fmt.Errorf("sending batch: %d entries failed, first: %s", len(res.Failed), aws.ToString(res.Failed[0].Message))
	}

	return nil
}

// sendTogether sends the same body copies times, from goroutines released at
// the same instant.
func sendTogether(ctx context.Context, client *sqs.Client, queueURL, body string, copies int) error {
	start := make(chan struct{})
	errs := make(chan error, copies)
	var wg sync.WaitGroup

	for range copies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: aws.String(body)})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return fmt.Errorf("sending race copy: %w", err)
		}
	}

	return nil
}

func init() {
	log.SetFlags(log.Ltime)
	log.SetOutput(os.Stderr)
}
