package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"golang.org/x/sync/errgroup"
	"os"
	"os/signal"
	"sort"
	"sync"
	"time"
)

var percentiles = []float32{
	0.5,
	0.66,
	0.75,
	0.8,
	0.9,
	0.95,
	0.98,
	0.99,
	1,
}

type Message struct {
	Exchange   string
	RoutingKey string
	Expiration int
	Size       int
	Index      int
}

type Stat struct {
	Message  Message
	Duration time.Duration
	Error    error
}

func interrupter(cancel func(os.Signal)) {
	s := make(chan os.Signal)
	signal.Notify(s, os.Interrupt, os.Kill)
	n := <-s
	cancel(n)
}

func worker(ctx context.Context, conn *amqp.Connection, in <-chan Message, out chan<- Stat) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	defer ch.Close()

	for {
		select {
		case m, ok := <-in:
			if !ok {
				return nil
			}

			start := time.Now()

			err := ch.Publish(m.Exchange, m.RoutingKey, false, false, amqp.Publishing{
				Body: bytes.Repeat([]byte("x"), m.Size),
			})

			d := time.Since(start)

			// check if we are existing before reporting errors
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			if err != nil {
				fmt.Printf("ERROR: Message publishing failed: %v\n", err)
			}

			select {
			case out <- Stat{
				Message:  m,
				Duration: d,
				Error:    err,
			}:
			case <-ctx.Done():
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func main() {
	var config struct {
		AMQPURL     string
		Concurrency int
		Messages    int
		Size        int
		Expiration  int
		Exchange    string
		RoutingKey  string
	}

	flag.StringVar(&config.AMQPURL, "amqp-url", "amqp://", "AMQP URL (see https://www.rabbitmq.com/uri-spec.html)")
	flag.IntVar(&config.Concurrency, "c", 100, "Number of concurrent publishers")
	flag.IntVar(&config.Messages, "n", 1000, "Total number of messages to publish")
	flag.IntVar(&config.Size, "size", 1024, "Message size (bytes)")
	flag.IntVar(&config.Expiration, "exp", 0, "Expiration time for messages (ms)")
	flag.StringVar(&config.RoutingKey, "routing-key", "messages", "Routing Key")
	flag.StringVar(&config.Exchange, "exchange", "", "Exchange")
	flag.Parse()

	if config.Messages == 0 || config.Concurrency == 0 {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	// connect to AMQP server
	conn, err := amqp.Dial(config.AMQPURL)
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	// setup sigint handler
	go interrupter(func(s os.Signal) {
		fmt.Printf("Signal %v is received, stopping...\n", s)
		cancel()
		conn.Close()
	})

	// publisher
	messages := make(chan Message, config.Concurrency)
	go func() {
		fmt.Printf("Sending messages (be patient)...\n")

		defer close(messages)

		for i := 0; i < config.Messages; i++ {
			select {
			case <-ctx.Done():
				return
			case messages <- Message{
				Index:      i,
				Exchange:   config.Exchange,
				RoutingKey: config.RoutingKey,
				Size:       config.Size,
				Expiration: config.Expiration,
			}:
			}
		}
	}()

	stats := make(chan Stat, config.Concurrency)

	// workers
	workereg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < config.Concurrency; i++ {
		workereg.Go(func() error {
			return worker(ctx, conn, messages, stats)
		})
	}

	// stats
	statswg := sync.WaitGroup{}
	statswg.Add(1)
	go func() {
		defer statswg.Done()

		times := make([]time.Duration, 0, config.Messages)

		var published, failures int
		var total time.Duration

		defer func() {
			sort.Slice(times, func(i, j int) bool {
				return times[i] < times[j]
			})

			fmt.Println()
			fmt.Printf("Concurrency level:\t%v\n", config.Concurrency)
			fmt.Printf("Time taken for tests:\t%v\n", total)
			fmt.Printf("Published messages:\t%v\n", published)
			fmt.Printf("Failed messages:\t%v\n", failures)

			if published != 0 {
				avg := total / time.Duration(published)
				fmt.Printf("Messages per second:\t%.2f (mean)\n", 1/avg.Seconds())
				fmt.Printf("Time per message:\t%v (mean)\n", avg)
				fmt.Printf("Time per message:\t%v (mean, across all concurrent publishers)\n", total/time.Duration(published*config.Concurrency))

				fmt.Println()
				fmt.Println("Percentage of the messages published within a certain time")
				for _, p := range percentiles {
					fmt.Printf("    %d%% \t%v\n", int(p*100), times[int(float32(len(times)-1)*p)])
				}
			}
		}()

		for {
			select {
			case s, ok := <-stats:
				if !ok {
					return
				}

				if s.Error == nil {
					published++
					total += s.Duration
					times = append(times, s.Duration)
				} else {
					failures++
				}

				if published%1000 == 0 {
					fmt.Printf("%v messages are published\n", published)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	workereg.Wait()
	close(stats)
	statswg.Wait()
}
