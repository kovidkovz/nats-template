package natstemplate

import (
	"fmt"
	"os"
	"github.com/nats-io/nats.go"
	"github.com/joho/godotenv"
	"log"
)

func NatsConnector() (*nats.Conn, nats.JetStreamContext, error){
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: No .env file found")
	}

	natsURL := os.Getenv("NATS_URL")
	natsUser := os.Getenv("NATS_USER")
	natsPass := os.Getenv("NATS_PASS")

	nc, err := nats.Connect(natsURL, nats.UserInfo(natsUser, natsPass))
	if err != nil {
		return nil, nil, fmt.Errorf("error connecting to NATS: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("error initializing JetStream: %w", err)
	}

	return nc, js, nil
}
