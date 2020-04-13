package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/Shopify/sarama"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var wg sync.WaitGroup
var kafkaTopic string
var mqttTopic string

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	mqttBroker := os.Getenv("MQTT_BROKER")
	mqttTopic = os.Getenv("MQTT_TOPIC")
	mqttClientID := os.Getenv("MQTT_CLIENT_ID")
	mqttUser := os.Getenv("MQTT_USER")
	mqttPassword := os.Getenv("MQTT_PASSWORD")

	kafkaServer := os.Getenv("KAFKA_SERVER")
	kafkaTopic = os.Getenv("KAFKA_TOPIC")
	kafkaClientID := os.Getenv("KAFKA_CLIENT_ID")
	kafkaUser := os.Getenv("KAFKA_USER")
	kafkaPassword := os.Getenv("KAFKA_PASSWORD")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s", mqttBroker))
	opts.SetUsername(mqttUser)
	opts.SetPassword(mqttPassword)
	opts.SetClientID(mqttClientID)
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
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
	loadNewData(client, producer)

	<-c
}

func loadNewData(client mqtt.Client, producer sarama.SyncProducer) {
	client.Subscribe(mqttTopic, 0, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Println("new data...")
		wg.Add(1)
		go saveToKafka(msg, producer)
	})
}

func saveToKafka(msg mqtt.Message, producer sarama.SyncProducer) {
	defer wg.Done()
	strTime := strconv.Itoa(int(time.Now().Unix()))
	kafkaMsg := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Key:   sarama.StringEncoder(strTime),
		Value: sarama.ByteEncoder(msg.Payload()),
	}
	_, _, err := producer.SendMessage(kafkaMsg)
	if err != nil {
		fmt.Println(err)
	}
}
