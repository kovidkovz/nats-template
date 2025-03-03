package main

import (
	"log"
	"time"

	"github.com/nats-io/nats.go"
)


func consumer(stream string, subject string, durable_name string, func messagehandler(msg *nats.Msg)) {
	nc, jetstream_consumer, err := NatsConnector()
	if err != nil {
		log.Fatalf("Error getting nats connection:", err)
	}

	defer nc.Close()

	// Define the ConsumerConfig equivalent
	consumerConfig := &nats.ConsumerConfig{
		DeliverPolicy: nats.DeliverAllPolicy, // Equivalent to DeliverPolicy.ALL
		AckPolicy:     nats.AckAllPolicy,     // Equivalent to AckPolicy.ALL
		MaxDeliver:    1024,                  // Equivalent to max_deliver=1024
		FilterSubject: subject,               // Equivalent to filter_subject
	}

	streams, err := jetstream_consumer.StreamsInfo()

	streamExists := false
	for _, s := range streams {
		if s.config.name == stream {
			streamExists = true
			break
		}
	}

	if !streamExists {
		panic("stream does not exist")
	}

	_, err := jetstream_consumer.AddConsumer(streamName, consumerConfig)
	if err != nil {
		log.Fatalf("Failed to add consumer: %v", err)
	}

	pull_subscriber, err := jetstream_consumer.PullSubscribe(subject, durable_name)
	if err!= nil{
		panic("Failed to create a pull subscriber")
	}

	msgs, err := pull_subscriber.FetchBatch(100, nats.MaxWait(5*time.Second))

}