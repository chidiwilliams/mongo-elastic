package sync

import (
	"context"
	"fmt"
	"log"

	"github.com/olivere/elastic"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

// TODO: Single sync function handling writing changestream.timestamp, calling dump and tail

// Dump indexes documents in the Mongo databases according to the given config.
// Each collection is indexed in a separate goroutine. The function waits for all
// goroutines to complete and returns the first error from the goroutines, if any.
func (d *dumper) Dump(ctx context.Context, syncMapping config.SyncMapping) error {
	// TODO: Save actions for tail call
	actions, err := d.syncActions(ctx, syncMapping)
	if err != nil {
		return err
	}

	for _, action := range actions {
		// Evaluate variables for goroutine call.
		// See: https://github.com/golang/go/wiki/CommonMistakes#using-goroutines-on-loop-iterator-variables
		coll, collMapping, dbMapping := action.coll, action.collMapping, action.dbMapping

		// Dump collection to an elastic index in a new goroutine.
		d.errGroup.Go(func() error {
			if err = d.dumpCollection(ctx, coll, collMapping, dbMapping); err != nil {
				return fmt.Errorf("indexing collection [%s]: %w", coll.Name(), err)
			}
			return nil
		})
	}

	// Wait for all goroutines to complete, and return the first error
	return d.errGroup.Wait()
}

// dumpCollection indexes all documents in the given collection to Elasticsearch.
// It returns errors that occur while creating the index or getting a cursor.
// TODO: If an error occurs wile indexing a document, return through a provided error channel and continue indexing.
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

	indexCount := 0
	for cursor.Next(ctx) {
		var doc map[string]interface{}
		if err = cursor.Decode(&doc); err != nil {
			return err
		}

		if err = d.indexDocument(ctx, idxName, doc, collMapping.Fields); err != nil {
			return err
		}
		indexCount += 1
	}

	fmt.Printf("Indexed %d document(s) in collection [%s] to index [%s]\n", indexCount, coll.Name(), idxName)
	return nil
}

func (d dumper) Tail(ctx context.Context, startUnix int64, syncMapping config.SyncMapping) error {
	indexErrs := make(chan error)

	actions, err := d.syncActions(ctx, syncMapping)
	if err != nil {
		return err
	}

	for _, action := range actions {
		go func(action syncAction) {
			if err = d.tailCollection(ctx, startUnix, action.coll, action.collMapping, action.dbMapping, indexErrs); err != nil {
				log.Println(fmt.Errorf("tailer died: collection [%s]: %w", action.coll.Name(), err))
			}
		}(action)
	}

	for {
		select {
		case err = <-indexErrs:
			log.Print(fmt.Errorf("tail error: %w", err))
		}
	}
}

func (d dumper) tailCollection(ctx context.Context, startUnix int64, coll *mongo.Collection, collMapping config.CollectionMapping, dbMapping config.DatabaseMapping, indexErrs chan<- error) error {
	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetStartAtOperationTime(&primitive.Timestamp{T: uint32(startUnix)})

	stream, err := coll.Watch(ctx, []bson.M{}, opts)
	if err != nil {
		return err
	}

	defer func() { printIfErr(stream.Close(ctx)) }()

	// TODO: Change logger to logrus
	fmt.Printf("collection [%s]: listening for new events on collection\n", coll.Name())

	for {
		fmt.Printf("collection [%s]: listening for next stream event\n", coll.Name())
		if !stream.Next(ctx) {
			// stream died, return deadline/cursor error
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return stream.Err()
		}

		evt := mongo2.ChangeStreamEvent{}
		if err = stream.Decode(&evt); err != nil {
			indexErrs <- fmt.Errorf("collection [%s]: %w", coll.Name(), err)
			continue
		}

		fmt.Printf("collection[%s]: received new stream event of type [%s]\n", coll.Name(), evt)

		if err = d.handleStreamEvent(ctx, evt, collMapping, dbMapping); err != nil {
			indexErrs <- fmt.Errorf("collection [%s]: %w", coll.Name(), err)
			continue
		}
	}
}

func (d dumper) handleStreamEvent(ctx context.Context, evt mongo2.ChangeStreamEvent, collMapping config.CollectionMapping, dbMapping config.DatabaseMapping) error {
	index := indexName(collMapping.Name, dbMapping.Name)

	// TODO: Handle all other event types, as listed in: https://docs.mongodb.com/manual/reference/change-events/#change-stream-output
	switch evt.OperationType {
	case mongo2.ChangeStreamEventOperationTypeInsert,
		mongo2.ChangeStreamEventOperationTypeReplace,
		mongo2.ChangeStreamEventOperationTypeUpdate:
		return d.indexDocument(ctx, index, evt.FullDocument, collMapping.Fields)
	case mongo2.ChangeStreamEventOperationTypeDelete:
		return d.deleteDocument(ctx, index, evt.DocumentKey.ID.Hex())
	}
	return nil
}

func (d dumper) indexDocument(ctx context.Context, index string, doc map[string]interface{}, fieldMapping []fields.M) error {
	id := doc["_id"].(primitive.ObjectID)

	doc, err := fields.Select(doc, fieldMapping)
	if err != nil {
		return fmt.Errorf("mapping document [%s]: %w", id.Hex(), err)
	}

	// _id is reserved as a metadata field in Elasticsearch and cannot be added to a document. Rename to id.
	doc["id"] = id
	delete(doc, "_id")

	_, err = d.elasticClient.Index().
		Index(index).Type(index).
		Id(id.Hex()).BodyJson(doc).Do(ctx)
	return err
}

func (d dumper) deleteDocument(ctx context.Context, index string, id string) error {
	_, err := d.elasticClient.Delete().Index(index).Type(index).Id(id).Do(ctx)
	return err
}

type syncAction struct {
	coll        *mongo.Collection
	collMapping config.CollectionMapping
	dbMapping   config.DatabaseMapping
}

func (d dumper) syncActions(ctx context.Context, syncMapping config.SyncMapping) ([]syncAction, error) {
	actions := make([]syncAction, 0)

	includedDBs := make(map[string]config.DatabaseMapping, len(syncMapping.Databases))
	for _, database := range syncMapping.Databases {
		includedDBs[database.Name] = database
	}

	listDatabasesResult, err := d.mongoClient.ListDatabases(ctx, bson.D{})
	if err != nil {
		return nil, err
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
			return nil, err
		}

		for _, collectionName := range collectionNames {
			if len(includedCollections) == 0 {
				collections[database.Collection(collectionName)] = config.CollectionMapping{Name: collectionName}
			} else if collConf, ok := includedCollections[collectionName]; ok {
				collections[database.Collection(collectionName)] = collConf
			}
		}

		for coll, collMapping := range collections {
			actions = append(actions, syncAction{coll: coll, collMapping: collMapping, dbMapping: dbMapping})
		}
	}

	return actions, nil
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
