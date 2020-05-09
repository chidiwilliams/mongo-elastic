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

var defaultMongoDbNames = []string{"admin", "config", "local"}

func isDefaultMongoDBName(s string) bool {
	for _, name := range defaultMongoDbNames {
		if name == s {
			return true
		}
	}
	return false
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	configPtr := flag.String("config", "config.yaml", "Configuration file")
	flag.Parse()

	conf := config{}
	if err := parseConfig(*configPtr, &conf); err != nil {
		return fmt.Errorf("error parsing config file: %w", err)
	}

	log.Println("config:", conf)

	mongoClient, err := connectMongo(conf.MongoURL)
	if err != nil {
		return fmt.Errorf("error connecting to mongo: %w", err)
	}

	log.Println("connected to mongoDB")

	elasticClient, err := connectElastic(conf.ElasticURL)
	if err != nil {
		return fmt.Errorf("error connecting to elastic: %w", err)
	}

	log.Println("connected to elastic")

	d := newDumper(mongoClient, elasticClient)
	if err = d.dump(context.Background(), conf); err != nil {
		return fmt.Errorf("error dumping from mongo to elastic: %w", err)
	}

	return nil
}

func connectElastic(url string) (*elastic.Client, error) {
	return elastic.NewClient(elastic.SetURL(url), elastic.SetSniff(false))
}

func connectMongo(url string) (*mongo.Client, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	return mongo.Connect(ctx, options.Client().ApplyURI(url))
}
