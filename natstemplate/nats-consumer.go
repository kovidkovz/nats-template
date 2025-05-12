package natstemplate

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type ConsumerOptions struct {
	Apikey string
}

func Consumer(stream string, subject string, durable_name string, frequency string, jetstream_consumer nats.JetStreamContext, messagehandler func(msg *nats.Msg, opts *ConsumerOptions), opts *ConsumerOptions) {
	// check for the stream
	streamInfo, err := jetstream_consumer.StreamInfo(stream)
	if err != nil {
		fmt.Println("stream does not exist", err)
		return
	}

	fmt.Println("Subjects of stream:", streamInfo.Config.Subjects)

	// check for the consumer info, so that duplicate consumer is not created
	_, err = jetstream_consumer.ConsumerInfo(stream, durable_name)
	if err != nil {
		// create a durable consumer
		// define consumer configuration
		consumerConfig := &nats.ConsumerConfig{
			DeliverPolicy: nats.DeliverAllPolicy,
			AckPolicy:     nats.AckExplicitPolicy,
			MaxDeliver:    1024,
			FilterSubject: subject,
			Durable:       durable_name,
		}

		// add consumer into the jetstream
		_, err = jetstream_consumer.AddConsumer(stream, consumerConfig)
		if err != nil {
			log.Fatalf("Failed to add consumer: %v", err)
		}
	}

	// pull subscribe to the subject for consuming messages associated/attached to this subject in this particular stream...
	pull_subscriber, err := jetstream_consumer.PullSubscribe(subject, durable_name)
	if err != nil {
		log.Fatalf("Failed to create a pull subscriber: %v", err)
	}

	fmt.Println("Consumer started and listening for messages...")

	// Infinite loop to keep processing messages
	for {
		if frequency != "" {
			// Fetch in batch mode
			batch, err := pull_subscriber.FetchBatch(100, nats.MaxWait(5*time.Second))
			if err != nil {
				if errors.Is(err, nats.ErrNoResponders) {
					log.Println("No responders available — stream or consumer may be deleted. Stopping consumption.")
					break // Stop processing
				} else {
					log.Println("Error fetching messages:", err)
					continue // Retry in case of other errors
				}
			}

			// Read messages from the channel
			for msg := range batch.Messages() { // Corrected: Read from the channel
				log.Println("Total Messages in batch:", len(batch.Messages()))
				messagehandler(msg, opts)
				
				err = msg.Ack()
				if err != nil {
					log.Println("Failed to ACK:", err)
				}
			}

		} else {
			// Low-frequency mode: Fetch one message at a time
			msgs, err := pull_subscriber.Fetch(1)
			if err != nil {
				if errors.Is(err, nats.ErrNoResponders) {
					log.Println("No responders available — stream or consumer may be deleted. Stopping consumption.")
					break // Stop processing
				} else {
					log.Println("Error fetching messages:", err)
					continue // Retry in case of other errors
				}
			}

			for _, msg := range msgs {
				messagehandler(msg, opts)
				err = msg.Ack()
				if err != nil {
					log.Println("Failed to ACK:", err)
				}
			}
		}
	}
}
