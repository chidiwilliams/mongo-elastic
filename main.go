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

	"mongo-elastic-sync/config"
	"mongo-elastic-sync/syncer"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	configPtr := flag.String("config", "config.yml", "Configuration file")
	flag.Parse()

	conf := config.Config{}
	if err := config.FromYamlFile(*configPtr, &conf); err != nil {
		return fmt.Errorf("parsing config file: %w", err)
	}

	mongoClient, err := connectMongo(conf.MongoURL)
	if err != nil {
		return fmt.Errorf("connecting to mongo: %w", err)
	}

	log.Println("connected to mongoDB")

	elasticClient, err := connectElastic(conf.ElasticURL)
	if err != nil {
		return fmt.Errorf("connecting to elastic: %w", err)
	}

	log.Println("connected to elastic")

	ctx := context.Background()
	syncMapping := config.SyncMapping{Databases: conf.Databases}
	return syncer.New(mongoClient, elasticClient).Sync(ctx, syncMapping)
}

func connectElastic(url string) (*elastic.Client, error) {
	return elastic.NewClient(elastic.SetURL(url), elastic.SetSniff(false))
}

func connectMongo(url string) (*mongo.Client, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	return mongo.Connect(ctx, options.Client().ApplyURI(url))
}
