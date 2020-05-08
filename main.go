package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/olivere/elastic"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	configPtr := flag.String("config", "config.yaml", "Configuration file")
	flag.Parse()

	conf := config{}
	err := parseConfig(*configPtr, &conf)
	if err != nil {
		log.Fatal(fmt.Errorf("error parsing config file: %w", err))
	}

	log.Println("config:", conf)

	mongoClient, err := connectMongo(conf.MongoURL)
	if err != nil {
		log.Fatal(fmt.Errorf("error connecting to mongo: %w", err))
	}

	log.Println("connected to mongoDB")

	elasticClient, err := connectElastic(conf.ElasticURL)
	if err != nil {
		log.Fatal(fmt.Errorf("error connecting to elastic: %w", err))
	}

	log.Println("connected to elastic")

	_, _ = mongoClient, elasticClient
}

func connectElastic(url string) (*elastic.Client, error) {
	return elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false),
	)
}

func connectMongo(url string) (*mongo.Client, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	return mongo.Connect(ctx, options.Client().ApplyURI(url))
}
