package main

import (
	"context"
	"fmt"
	"log"

	"github.com/olivere/elastic"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"

	"mongo-elastic/fields"
)

func newDumper(mongoClient *mongo.Client, elasticClient *elastic.Client) *dumper {
	return &dumper{mongoClient: mongoClient, elasticClient: elasticClient}
}

type dumper struct {
	mongoClient   *mongo.Client
	elasticClient *elastic.Client
	errGroup      errgroup.Group
}

func (d *dumper) dump(ctx context.Context, config config) error {
	includedDBs := make(map[string]databaseConfig, len(config.Databases))
	for _, database := range config.Databases {
		includedDBs[database.Name] = database
	}

	listDatabasesResult, err := d.mongoClient.ListDatabases(ctx, bson.D{})
	if err != nil {
		return err
	}

	databases := make(map[*mongo.Database]databaseConfig, len(listDatabasesResult.Databases))
	for _, database := range listDatabasesResult.Databases {
		if dbConf, ok := includedDBs[database.Name]; !isDefaultMongoDBName(database.Name) && (len(includedDBs) == 0 || ok) {
			databases[d.mongoClient.Database(database.Name)] = dbConf
		}
	}

	for database, dbConf := range databases {
		collections := make(map[*mongo.Collection]collectionConfig)

		includedCollections := make(map[string]collectionConfig, len(dbConf.Collections))
		for _, collection := range dbConf.Collections {
			includedCollections[collection.Name] = collection
		}

		collectionNames, err := database.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return err
		}

		for _, collectionName := range collectionNames {
			if collConf, ok := includedCollections[collectionName]; len(includedCollections) == 0 || ok {
				collections[database.Collection(collectionName)] = collConf
			}
		}

		for coll, collConf := range collections {
			// Evaluate variables for goroutine call.
			// See: https://github.com/golang/go/wiki/CommonMistakes#using-goroutines-on-loop-iterator-variables
			ctx, coll, collConf, dbConf := ctx, coll, collConf, dbConf

			// Dump collection to an elastic index in a new goroutine.
			d.errGroup.Go(func() error {
				return d.dumpCollection(ctx, coll, collConf, dbConf)
			})
		}

		log.Printf("completed indexing for database [%s]\n", dbConf.Name)
	}

	if err = d.errGroup.Wait(); err != nil {
		log.Fatal(err)
	}

	log.Println("completed indexing for all databases")
	return nil
}

// dumpCollection indexes all documents in the given collection to Elasticsearch.
func (d *dumper) dumpCollection(ctx context.Context, coll *mongo.Collection, collConfig collectionConfig, dbConfig databaseConfig) error {
	idxName := indexName(collConfig.Name, dbConfig.Name)
	log.Printf("dumping collection [%s] in database [%s] to elastic index [%s]", collConfig.Name, dbConfig.Name, idxName)

	idxExists, err := d.elasticClient.IndexExists(idxName).Do(ctx)
	if err != nil {
		return err
	}

	if idxExists {
		log.Printf("elastic index [%s] already exists, skipping create", idxName)
	} else {
		log.Printf("elastic index [%s] does not exist, creating", idxName)
		_, err = d.elasticClient.CreateIndex(idxName).Do(ctx)
		if err != nil {
			return err
		}
		log.Printf("elastic index [%s] created", idxName)
	}

	cursor, err := coll.Find(ctx, bson.D{})
	if err != nil {
		return nil
	}

	defer func() { printIfErr(cursor.Close(ctx)) }()

	var doc map[string]interface{}
	for cursor.Next(ctx) {
		if err = cursor.Decode(&doc); err != nil {
			return err
		}

		id := doc["_id"].(primitive.ObjectID)

		// _id is reserved as a metadata field in Elasticsearch and cannot be added to a document. Rename to id.
		doc["id"] = doc["_id"]
		delete(doc, "_id")

		doc, err = fields.Select(doc, collConfig.Fields)
		if err != nil {
			return fmt.Errorf("mapping document [%s]: %w", id.Hex(), err)
		}

		_, err = d.elasticClient.Index().
			Index(idxName).Type(idxName).
			Id(id.Hex()).BodyJson(doc).Do(ctx)
		if err != nil {
			return err
		}

		log.Printf("indexed document [%s]\n", id.Hex())
	}

	log.Printf("completed indexing for collection [%s] to index:%s\n", coll.Name(), idxName)
	return nil
}

func indexName(collName, dbName string) string {
	return fmt.Sprintf("%s.%s", collName, dbName)
}

func printIfErr(err error) {
	if err != nil {
		log.Println(err)
	}
}
