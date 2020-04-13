package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/joho/godotenv"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/Shopify/sarama"
	"github.com/robfig/cron/v3"
)

var (
	mongoDatabase    string
	mongoCollection  string
	mongoFilterKey   string
	mongoFilterValue string
	kafkaTopic       string
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	mongoURL := os.Getenv("MONGO_URL")
	mongoDatabase = os.Getenv("MONGO_DATABASE")
	mongoCollection = os.Getenv("MONGO_COLLECTION")
	mongoFilterKey = os.Getenv("MONGO_FILTER_KEY")
	mongoFilterValue = os.Getenv("MONGO_FILTER_VALUE")
	queryInterval := fmt.Sprintf("@every %s", os.Getenv("MONGO_QUERY_INTERVAL"))

	kafkaServer := os.Getenv("KAFKA_SERVER")
	kafkaTopic = os.Getenv("KAFKA_TOPIC")
	kafkaClientID := os.Getenv("KAFKA_CLIENT_ID")
	kafkaUser := os.Getenv("KAFKA_USER")
	kafkaPassword := os.Getenv("KAFKA_PASSWORD")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Hour)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURL))
	if err != nil {
		log.Fatal(err)
	}

	conf := sarama.NewConfig()
	conf.Version = sarama.V0_10_0_1
	conf.ClientID = kafkaClientID
	conf.Net.SASL.Enable = true
	conf.Net.SASL.User = kafkaUser
	conf.Net.SASL.Password = kafkaPassword
	conf.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypePlaintext)
	conf.Net.SASL.Handshake = true
	conf.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{kafkaServer}, conf)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	c := cron.New(cron.WithSeconds())
	c.AddFunc(queryInterval, func() { loadNewData(ctx, client, producer) })
	c.Start()
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, os.Kill)
	<-sig
	for {
	}
}

func loadNewData(ctx context.Context, client *mongo.Client, producer sarama.SyncProducer) {
	db := client.Database(mongoDatabase)
	collection := db.Collection(mongoCollection)

	filter := bson.M{mongoFilterKey: mongoFilterValue}
	cur, _ := collection.Find(ctx, filter)
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var result bson.D
		var temporaryBytes []byte
		err := cur.Decode(&result)
		if err != nil {
			fmt.Println(err)
		}
		temporaryBytes, err = bson.MarshalExtJSON(result, true, true)
		if err != nil {
			fmt.Println(err)
		}
		saveToKafka(temporaryBytes, producer)
	}
}

func saveToKafka(msg []byte, producer sarama.SyncProducer) {
	strTime := strconv.Itoa(int(time.Now().Unix()))
	kafkaMsg := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Key:   sarama.StringEncoder(strTime),
		Value: sarama.ByteEncoder(msg),
	}
	fmt.Println("Writing new data to Kafka...")
	_, _, err := producer.SendMessage(kafkaMsg)
	if err != nil {
		fmt.Println(err)
	}
}
