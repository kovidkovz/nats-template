package main

import (
	"log"
	"time"
	"github.com/nats-io/nats.go"
)

func consumer(stream string, subject string, durable_name string, frequency string, messagehandler func(msg *nats.Msg)) {
	nc, jetstream_consumer, err := NatsConnector()
	if err != nil {
		log.Fatalf("Error getting nats connection: %v", err)
	}

	defer nc.Close()

	// Define ConsumerConfig
	consumerConfig := &nats.ConsumerConfig{
		DeliverPolicy: nats.DeliverAllPolicy, // Equivalent to DeliverPolicy.ALL
		AckPolicy:     nats.AckAllPolicy,     // Equivalent to AckPolicy.ALL
		MaxDeliver:    1024,                  // Equivalent to max_deliver=1024
		FilterSubject: subject,               // Equivalent to filter_subject
	}

	// Check if stream exists
	streams:= jetstream_consumer.StreamsInfo()

	streamExists := false
	for streamInfo := range streams {
		if streamInfo.Config.Name == stream { // ✅ Correct field names
			streamExists = true
			break
		}
	}

	if !streamExists {
		log.Fatalf("Stream does not exist: %s", stream)
	}

	_, err = jetstream_consumer.AddConsumer(stream, consumerConfig)
	if err != nil {
		log.Fatalf("Failed to add consumer: %v", err)
	}

	pull_subscriber, err := jetstream_consumer.PullSubscribe(subject, durable_name)
	if err != nil {
		log.Fatalf("Failed to create a pull subscriber: %v", err)
	}

	if frequency != "" {
		batch, err := pull_subscriber.FetchBatch(100, nats.MaxWait(5*time.Second)) 
		if err != nil {
			log.Println("Error fetching messages:", err)
			return
		}
		
		for msg := range batch.Messages() {
			messagehandler(msg)
			msg.Ack() 
		}

	} else {
		msgs, err := pull_subscriber.Fetch(1)
		if err != nil {
			log.Println("Error fetching messages:", err)
			return
		}

		for _, msg := range msgs {
			messagehandler(msg)
			msg.Ack()
		}
	}	
}
