package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

var val string

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	server := os.Getenv("KAFKA_SERVER")
	topic := os.Getenv("KAFKA_TOPIC")
	clientID := os.Getenv("KAFKA_CLIENT_ID")
	user := os.Getenv("KAFKA_USER")
	password := os.Getenv("KAFKA_PASSWORD")
	httpAddress := os.Getenv("HTTP_SERVE_ADDRESS")

	http.HandleFunc("/", handle)
	go http.ListenAndServe(httpAddress, nil)

	ctx := context.Background()

	sasl := plain.Mechanism{
		Username: user,
		Password: password,
	}

	d := kafka.Dialer{
		ClientID:      clientID,
		SASLMechanism: sasl,
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{server},
		Topic:       topic,
		StartOffset: kafka.LastOffset,
		Dialer:      &d,
		Logger:      log.New(os.Stderr, "kafka ", log.LstdFlags),
	})

	log.Println("Application successfully started...")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatal(err)
		}
		val = string(msg.Value)
	}
}

func handle(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, val)
}
