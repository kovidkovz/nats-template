package main

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

func NatsConnector() (*nats.Conn, nats.JetStreamContext, error){
	fmt.Println("Initializing connection to the server....")
	nc, err := nats.Connect("nats://localhost:4222")

	if err != nil {
		return nil, nil, fmt.Errorf("error connecting to NATS: %w", err)
	}

	js, err := nc.JetStream()

	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("error initializing the jetstream: %w", err)
	}

	return nc, js, nil
}
