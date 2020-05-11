package sync

import (
	"context"
	"fmt"
	"log"

	"github.com/olivere/elastic"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"

	"mongo-elastic-sync/config"
	"mongo-elastic-sync/fields"
	mongo2 "mongo-elastic-sync/mongo"
)

// NewDumper returns a new dumper.
func NewDumper(mongoClient *mongo.Client, elasticClient *elastic.Client) *dumper {
	return &dumper{mongoClient: mongoClient, elasticClient: elasticClient}
}

// dumper dumps documents from Mongo into Elasticsearch.
type dumper struct {
	mongoClient   *mongo.Client
	elasticClient *elastic.Client
	errGroup      errgroup.Group
}

// Dump indexes documents in the Mongo databases according to the given config.
// Each collection is indexed in a separate goroutine. The function waits for all
// goroutines to complete and returns the first error from the goroutines, if any.
func (d *dumper) Dump(ctx context.Context, syncMapping config.SyncMapping) error {
	includedDBs := make(map[string]config.DatabaseMapping, len(syncMapping.Databases))
	for _, database := range syncMapping.Databases {
		includedDBs[database.Name] = database
	}

	listDatabasesResult, err := d.mongoClient.ListDatabases(ctx, bson.D{})
	if err != nil {
		return err
	}

	databases := make(map[*mongo.Database]config.DatabaseMapping, len(listDatabasesResult.Databases))
	for _, database := range listDatabasesResult.Databases {
		if !mongo2.IsSystemDB(database.Name) {
			if len(includedDBs) == 0 {
				databases[d.mongoClient.Database(database.Name)] = config.DatabaseMapping{Name: database.Name}
			} else if dbConf, ok := includedDBs[database.Name]; ok {
				databases[d.mongoClient.Database(database.Name)] = dbConf
			}
		}
	}

	for database, dbMapping := range databases {
		collections := make(map[*mongo.Collection]config.CollectionMapping)

		includedCollections := make(map[string]config.CollectionMapping, len(dbMapping.Collections))
		for _, collection := range dbMapping.Collections {
			includedCollections[collection.Name] = collection
		}

		collectionNames, err := database.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return err
		}

		for _, collectionName := range collectionNames {
			if len(includedCollections) == 0 {
				collections[database.Collection(collectionName)] = config.CollectionMapping{Name: collectionName}
			} else if collConf, ok := includedCollections[collectionName]; ok {
				collections[database.Collection(collectionName)] = collConf
			}
		}

		for coll, collMapping := range collections {
			// Evaluate variables for goroutine call.
			// See: https://github.com/golang/go/wiki/CommonMistakes#using-goroutines-on-loop-iterator-variables
			ctx, coll, collMapping, dbMapping := ctx, coll, collMapping, dbMapping

			// Dump collection to an elastic index in a new goroutine.
			d.errGroup.Go(func() error {
				if err = d.dumpCollection(ctx, coll, collMapping, dbMapping); err != nil {
					return fmt.Errorf("indexing collection [%s]: %w", coll.Name(), err)
				}
				return nil
			})
		}

		log.Printf("completed indexing for database [%s]\n", dbMapping.Name)
	}

	// Wait for all goroutines to complete, and return the first error
	return d.errGroup.Wait()
}

// dumpCollection indexes all documents in the given collection to Elasticsearch.
// It returns errors that occur while creating the index or getting a cursor.
// TODO: If an error occurs wile indexing a document, it is returned through a provided channel and indexing continues.
func (d *dumper) dumpCollection(ctx context.Context, coll *mongo.Collection, collMapping config.CollectionMapping, dbMapping config.DatabaseMapping) error {
	idxName := indexName(collMapping.Name, dbMapping.Name)
	log.Printf("dumping collection [%s] in database [%s] to elastic index [%s]", collMapping.Name, dbMapping.Name, idxName)

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

	for cursor.Next(ctx) {
		var doc map[string]interface{}
		if err = cursor.Decode(&doc); err != nil {
			return err
		}

		// TODO: Handle non-ObjectID ids
		id := doc["_id"].(primitive.ObjectID)

		doc, err = fields.Select(doc, collMapping.Fields)
		if err != nil {
			return fmt.Errorf("mapping document [%s]: %w", id.Hex(), err)
		}

		// _id is reserved as a metadata field in Elasticsearch and cannot be added to a document. Rename to id.
		doc["id"] = doc["_id"]
		delete(doc, "_id")

		_, err = d.elasticClient.Index().
			Index(idxName).Type(idxName).
			Id(id.Hex()).BodyJson(doc).Do(ctx)
		if err != nil {
			return err
		}

		log.Printf("indexed document [%s]\n", id.Hex())
	}

	log.Printf("completed indexing for collection [%s] to index [%s]\n", coll.Name(), idxName)
	return nil
}

// indexName returns the Elasticsearch index name for the given Mongo collection and database.
func indexName(collName, dbName string) string {
	return fmt.Sprintf("%s.%s", dbName, collName)
}

func printIfErr(err error) {
	if err != nil {
		log.Println(err)
	}
}
