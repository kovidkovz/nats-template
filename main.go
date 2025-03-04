package main

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

func main() {
	fmt.Println("Welcome to nats")

	subject := "test.subject"
	streamName := "test_stream"
	durableName := "test_durable"

	messageHandler := func(msg *nats.Msg) {
		fmt.Printf("Processing message: %s\n", string(msg.Data))
	}

	consumer(streamName, subject, durableName, "", messageHandler)
}
