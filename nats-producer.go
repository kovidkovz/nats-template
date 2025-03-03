package main

import (
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

// Producer struct manages NATS JetStream connections and publishing
type Producer struct {
	natsConn          *nats.Conn
	jetstreamProducer nats.JetStreamContext
}

// InitializeConnection establishes a connection to NATS and JetStream
func (p *Producer) InitializeConnection() error {
	// Call the exported GetNATSConnection function
	nc, js, err := NatsConnector()
	if err != nil {
		return fmt.Errorf("error getting NATS connection: %w", err)
	}

	fmt.Println("NATS Connected!")
	p.natsConn = nc
	p.jetstreamProducer = js
	return nil
}

// Produce publishes a message to a given NATS subject
func (p *Producer) Produce(subject string, msg string) error {
	if p.natsConn == nil || p.jetstreamProducer == nil {
		return fmt.Errorf("NATS connection or JetStream producer is not initialized")
	}

	// Publish the message
	ack, err := p.jetstreamProducer.Publish(subject, []byte(msg))
	if err != nil {
		log.Printf("Error publishing message: %v", err)
		return err
	}

	log.Printf("Message ACK: %+v\n", ack)
	return nil
}

// CloseConnection closes the NATS connection
func (p *Producer) CloseConnection() {
	if p.natsConn != nil {
		p.natsConn.Close()
		log.Println("NATS connection closed successfully")
	}
}

// Global Producer instance (renamed to avoid redeclaration)
var NatsProducerInstance = &Producer{} // Renamed to avoid conflict