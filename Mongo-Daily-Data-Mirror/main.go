package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"os"

	"github.com/jinzhu/now"

	"github.com/joho/godotenv"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var collectionTimestamp map[string]string

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	sourceURL := os.Getenv("MONGO_SOURCE_URL")
	destinationURL := os.Getenv("MONGO_DESTINATION_URL")

	collectionTimestamp = make(map[string]string)
	collectionTimestamp["values"] = "timeStamp"
	collectionTimestamp["values_actual"] = "timeStamp"
	collectionTimestamp["values_ncprogram"] = "timeStamp"
	collectionTimestamp["program_history"] = "start"
	collectionTimestamp["values_alarms"] = "alarmBeginTimeStamp"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Hour)
	defer cancel()

	source, err := mongo.Connect(ctx, options.Client().ApplyURI(sourceURL))
	if err != nil {
		log.Fatal(err)
	}

	destination, err := mongo.
		Connect(ctx, options.Client().ApplyURI(destinationURL))
	if err != nil {
		log.Fatal(err)
	}

	loadNewData(ctx, source, destination)
}

func loadNewData(ctx context.Context, client, mirror *mongo.Client) {
	dbs, _ := client.ListDatabaseNames(ctx, bson.D{})

	var nrValuesWritten int

	for _, d := range dbs {
		db := client.Database(d)
		collections, _ := db.ListCollectionNames(ctx, bson.D{})

		for _, c := range collections {
			collection := client.Database(d).Collection(c)

			timestampName := "timeStamp"
			if val, ok := collectionTimestamp[collection.Name()]; ok {
				timestampName = val
			}

			filter := bson.M{timestampName: bson.M{"$gte": now.BeginningOfDay(), "$lt": now.EndOfDay()}}
			cur, _ := collection.Find(ctx, filter)
			defer cur.Close(ctx)

			for cur.Next(ctx) {
				var result bson.M
				err := cur.Decode(&result)
				if err != nil {
					log.Fatal(err)
				}
				nrValuesWritten++
				saveToMirror(ctx, mirror, d, c, result)
			}
		}
	}
	fmt.Printf("Successfully wrote %d to mirror data-lake", nrValuesWritten)
}

func saveToMirror(ctx context.Context, mirror *mongo.Client, database, collection string, message bson.M) {
	c := mirror.Database(database).Collection(collection)
	c.InsertOne(ctx, message)
}
