package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

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
	//clientID := os.Getenv("KAFKA_CLIENT_ID")
	user := os.Getenv("KAFKA_USER")
	password := os.Getenv("KAFKA_PASSWORD")

	http.HandleFunc("/", handle)
	go http.ListenAndServe(":8090", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sasl := plain.Mechanism{
		Username: user,
		Password: password,
	}

	d := kafka.Dialer{
		SASLMechanism: sasl,
	}
	conn, err := d.DialLeader(ctx, "tcp", server, topic, 0)
	if err != nil {
		log.Fatal(err)
	}

	for {
		msg, err := conn.ReadMessage(1e6) // Read max 1MB
		if err != nil {
			break
		}
		val = string(msg.Value)
	}
	conn.Close()
}

func handle(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, val)
}
