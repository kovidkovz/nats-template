package natstemplate

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func Consumer(stream string, subject string, durable_name string, frequency string, messagehandler func(msg *nats.Msg)) {
	_, jetstream_consumer, err := NatsConnector()
	if err != nil {
		log.Fatalf("Error getting nats connection: %v", err)
	}

	consumerConfig := &nats.ConsumerConfig{
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckAllPolicy,
		MaxDeliver:    1024,
		FilterSubject: subject,
	}

	_, err = jetstream_consumer.StreamInfo(stream)
	if err != nil {
		fmt.Println("stream does not exist", err)
		return
	}

	_, err = jetstream_consumer.AddConsumer(stream, consumerConfig)
	if err != nil {
		log.Fatalf("Failed to add consumer: %v", err)
	}

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
				log.Println("Error fetching messages:", err)
				continue // Don't exit, just retry
			}

			// Read messages from the channel
			for msg := range batch.Messages() { // Corrected: Read from the channel
				messagehandler(msg)
				msg.Ack()
			}

		} else {
			// Low-frequency mode: Fetch one message at a time
			msgs, err := pull_subscriber.Fetch(1)
			if err != nil {
				log.Println("Error fetching messages:", err)
				continue // Retry fetching messages
			}

			for _, msg := range msgs {
				messagehandler(msg)
				msg.Ack()
			}
		}
	}
}
