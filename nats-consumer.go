package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func consumer(stream string, subject string, durable_name string, frequency string, messagehandler func(msg *nats.Msg)) {
	nc, jetstream_consumer, err := NatsConnector()
	if err != nil {
		log.Fatalf("Error getting nats connection: %v", err)
	}

	defer nc.Close() //The defer keyword is used to execute the function at the end of the main function. In this case the nc.close will get executed just before the last curly brace.}

	// Define ConsumerConfig
	// we can manually also add a consumer that will make things even faster
	consumerConfig := &nats.ConsumerConfig{
		DeliverPolicy: nats.DeliverAllPolicy, // Equivalent to DeliverPolicy.ALL
		AckPolicy:     nats.AckAllPolicy,     // Equivalent to AckPolicy.ALL
		MaxDeliver:    1024,                  // Equivalent to max_deliver=1024
		FilterSubject: subject,               // Equivalent to filter_subject
	}

	// Check if stream exists
	streams := jetstream_consumer.StreamsInfo()

	// jetstream_consumer.AddStream()

	streamExists := false
	for streamInfo := range streams {
		fmt.Println(streamInfo.Config.Name)
		if streamInfo.Config.Name == stream { // Correct field names
			streamExists = true
			break
		}
	}

	// check for the existence of stream
	if !streamExists {
		log.Fatalf("Stream does not exist: %s", stream)
	}

	// create consumer, this line of code will be executed each time the consumer function is called.
	// we can check for already created cosumers, but it will consume time, hence we will let nats handle it itself, as nats will only throw an error if a consumer with the same name is being created at the same time.
	_, err = jetstream_consumer.AddConsumer(stream, consumerConfig)
	if err != nil {
		log.Fatalf("Failed to add consumer: %v", err)
	}

	// consumer will pull subscribe to the subject
	pull_subscriber, err := jetstream_consumer.PullSubscribe(subject, durable_name)
	if err != nil {
		log.Fatalf("Failed to create a pull subscriber: %v", err)
	}

	// Based on the frequency of messages, the consumer or pullsubscriber will fetch the messages.
	// if the frequency is set to high cosumer will fetch in batch of 100 and a timeout of 5 seconds is set.
	// if the timeout reaches its threshold the consumer will consume all those messages that are less than 100 and process them.
	// this can be time consuming, if the frequency of messages is not high as the consumer will keep waiting for 100 messages for 5 seconds.
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
		// when the frequency is low fetch one message at a time
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
