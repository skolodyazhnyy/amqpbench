package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/skolodyazhnyy/amqp-cgi-bridge/log"
	"github.com/streadway/amqp"
	"golang.org/x/sync/errgroup"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"time"
)

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

func worker(ctx context.Context, logger *log.Logger, conn *amqp.Connection, in <-chan Message, out chan<- Stat) error {
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

			if err != nil {
				logger.Errorf("Message publishing failed: %v", err)
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
		LogFormat   string
		Concurrency int
		Messages    int
		Size        int
		Expiration  int
		Exchange    string
		RoutingKey  string
		Stats       string
	}

	flag.StringVar(&config.AMQPURL, "amqp-url", "amqp://", "AMQP URL (see https://www.rabbitmq.com/uri-spec.html)")
	flag.StringVar(&config.LogFormat, "log-fmt", "text", "Log format: text or json")
	flag.IntVar(&config.Concurrency, "c", 100, "Number of concurrent publishers")
	flag.IntVar(&config.Messages, "n", 1000, "Total number of messages to publish")
	flag.IntVar(&config.Size, "size", 1024, "Message size (bytes)")
	flag.IntVar(&config.Expiration, "exp", 0, "Expiration time for messages (ms)")
	flag.StringVar(&config.RoutingKey, "routing-key", "messages", "Routing Key")
	flag.StringVar(&config.Exchange, "exchange", "", "Exchange")
	flag.StringVar(&config.Stats, "stats", "stats.csv", "File path to save all stats")
	flag.Parse()

	logger := log.NewX(config.LogFormat, os.Stderr, log.DefaultTextFormat)

	var statsw = ioutil.Discard

	// open stats writer
	if config.Stats != "" {
		f, err := os.Create(config.Stats)
		if err != nil {
			logger.Fatal(err)
		}

		defer f.Close()

		statsw = f
	}

	ctx, cancel := context.WithCancel(context.Background())

	logger.Infof("Connecting to AMQP server...")

	// connect to AMQP server
	conn, err := amqp.Dial(config.AMQPURL)
	if err != nil {
		logger.Fatal(err)
	}

	logger.Infof("Connection to AMQP server established")

	defer conn.Close()

	// setup sigint handler
	go interrupter(func(s os.Signal) {
		logger.Infof("Signal %v is received, stopping...", s)
		conn.Close()
		cancel()
	})

	// publisher
	messages := make(chan Message)
	go func() {
		logger.Infof("Starting sending messages...")

		defer close(messages)

		for i := 0; i < config.Messages; i++ {
			m := Message{
				Index:      i,
				Exchange:   config.Exchange,
				RoutingKey: config.RoutingKey,
				Size:       config.Size,
				Expiration: config.Expiration,
			}

			select {
			case <-ctx.Done():
				return
			case messages <- m:
			}
		}
	}()

	// workers
	stats := make(chan Stat)
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < config.Concurrency; i++ {
		i := i

		eg.Go(func() error {
			return worker(ctx, logger.With(log.R{"worker": i}), conn, messages, stats)
		})
	}

	// stats
	go func() {
		n := 0
		total := time.Duration(0)
		max := time.Duration(0)
		min := time.Duration(0)
		start := time.Now()

		defer func() {
			logger.Infof("Messages sent: %v", n)
			logger.Infof("Max publish time: %v", max)
			logger.Infof("Min publish time: %v", min)
			logger.Infof("Avg publish time: %v", total/time.Duration(n))
			logger.Infof("Total duration: %v", time.Since(start))
		}()

		for {
			select {
			case s, ok := <-stats:
				if !ok {
					return
				}

				n++

				total += s.Duration

				if s.Duration > max {
					max = s.Duration
				}

				if s.Duration < min || min == 0 {
					min = s.Duration
				}

				errstr := ""
				if s.Error != nil {
					errstr = s.Error.Error()
				}

				cols := []string{
					fmt.Sprint(s.Message.Index),
					s.Message.Exchange,
					s.Message.RoutingKey,
					fmt.Sprint(s.Message.Size),
					fmt.Sprint(s.Message.Expiration),
					fmt.Sprint(int64(s.Duration)),
					errstr,
				}

				fmt.Fprintln(statsw, strings.Join(cols, ";"))

				if n%1000 == 0 {
					logger.Infof("%v messages are published", n)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// wait for workers to finish
	eg.Wait()

	close(stats)
}
