package natstemplate

import (
	"encoding/json"
	"log"
	"os"

	"github.com/nats-io/nats.go"
)

func Dynamic_Consumer_Creation(
	subject_prefix string,
	stream_prefix string,
	frequency string,
	js nats.JetStreamContext,
	messagehandler func(msg *nats.Msg, opts *ConsumerOptions),
) {
	// Get the key-value bucket
	kv, err := js.KeyValue(os.Getenv("BUCKET"))
	env := os.Getenv("ENV")
	if err != nil {
		log.Println("Error fetching bucket:", err)
		return
	}

	// Watch for key updates
	watcher, err := kv.Watch("*")
	if err != nil {
		log.Println("Error setting up KV watch:", err)
		return
	}

	go func() {
		for update := range watcher.Updates() {
			if update == nil || update.Operation() != nats.KeyValuePut {
				continue
			}

			subject := subject_prefix + update.Key() + ".*"
			durable := "consumer_" + update.Key() + env
			stream := stream_prefix + update.Key()

			var kvValues map[string]interface{}
			if err := json.Unmarshal(update.Value(), &kvValues); err != nil {
				log.Println("Error decoding KV JSON:", err)
				continue
			}

			apiKey, ok := kvValues["combain_customer_api_key"].(string)
			if !ok {
				log.Println("API key not found or not a string")
				continue
			}

			go Consumer(stream, subject, durable, frequency, js, messagehandler, apiKey)

		}
	}()

	select {} // Block forever
}