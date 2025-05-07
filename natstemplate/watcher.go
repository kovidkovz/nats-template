package natstemplate

import (
	"log"
	"os"

	"github.com/nats-io/nats.go"
)

func Dynamic_Consumer_Creation(subject_prefix string, stream_prefix string, frequency string, messagehandler func(msg *nats.Msg)) {
	// connect to the nats server
	_, js, _ := NatsConnector()

	kv, err := js.KeyValue(os.Getenv("BUCKET"))
	if err != nil {
		log.Println("Error fetching bucket...", err)
	}

	// Start watching for new tokens and also read the older ones to start consuming...
	watcher, _ := kv.Watch("*")
	go func() {
		for update := range watcher.Updates() {
			if update == nil || update.Operation() != nats.KeyValuePut {
				continue
			}
			subject := subject_prefix + update.Key() + ".*"
			durable := "consumer_" + update.Key()
			stream := stream_prefix + update.Key()

			go Consumer(stream, subject, durable, frequency, js, messagehandler)
		}
	}()

	select {} // Block forever
}