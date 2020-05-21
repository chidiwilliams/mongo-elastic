package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/olivere/elastic"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"mongo-elastic-sync/syncer"
)

const (
	testConfigPath = "config.test.yml"
)

var (
	mongoURL         = getEnvOrDefault("MONGO_URL", "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0&readPreference=primary")
	elasticSearchURL = getEnvOrDefault("ELASTICSEARCH_URL", "http://localhost:9200")
)

// map of db => collection => list of documents
type dbSeed map[string]map[string][]interface{}

// map of index => list of elastic docs
type result map[string][]elasticDoc

type elasticDoc struct {
	_id    string
	source map[string]interface{}
}

// db seed document
type d map[string]interface{}

func TestMain(m *testing.M) {
	defer os.Exit(m.Run())
}

func TestRun(t *testing.T) {
	mongoClient, err := connectMongo(mongoURL)
	fatalIfErr(t, err)

	elasticClient, err := connectElastic(elasticSearchURL)
	fatalIfErr(t, err)

	ctx := context.Background()
	reset(ctx, t, mongoClient, elasticClient)

	tests := []struct {
		desc   string
		config string
		seed   dbSeed
		result result
	}{
		{
			desc: "index all",
			config: fmt.Sprintf(`mongoURL: %s
elasticURL: %s
`, mongoURL, elasticSearchURL),
			seed: dbSeed{
				"db1": {
					"coll1": {d{"_id": oid("5eb6bd2d0b6bdf6514bb837c"), "a": "1"}, d{"_id": oid("5eb6bd440b6bdf6514bb8440"), "b": "2"}},
					"coll2": {d{"_id": oid("5eb6bd440b6bdf6514bb83e1"), "c": "5"}, d{"_id": oid("5eb6bd440b6bdf6514bb8410"), "d": "9"}},
				},
				"db2": {
					"coll3": {d{"_id": oid("5eb6bd440b6bdf6514bb843f"), "a": "1"}, d{"_id": oid("5eb6bd2d0b6bdf6514bb8397"), "b": "2"}},
					"coll4": {d{"_id": oid("5eb6bd440b6bdf6514bb8415"), "a": "1"}, d{"_id": oid("5eb6bd440b6bdf6514bb8442"), "b": "2"}},
				},
			},
			result: result{
				"db1.coll1": {
					{"5eb6bd2d0b6bdf6514bb837c", d{"a": "1", "id": "5eb6bd2d0b6bdf6514bb837c"}},
					{"5eb6bd440b6bdf6514bb8440", d{"b": "2", "id": "5eb6bd440b6bdf6514bb8440"}},
				},
				"db1.coll2": {
					{"5eb6bd440b6bdf6514bb83e1", d{"c": "5", "id": "5eb6bd440b6bdf6514bb83e1"}},
					{"5eb6bd440b6bdf6514bb8410", d{"d": "9", "id": "5eb6bd440b6bdf6514bb8410"}}},
				"db2.coll3": {
					{"5eb6bd440b6bdf6514bb843f", d{"a": "1", "id": "5eb6bd440b6bdf6514bb843f"}},
					{"5eb6bd2d0b6bdf6514bb8397", d{"b": "2", "id": "5eb6bd2d0b6bdf6514bb8397"}}},
				"db2.coll4": {
					{"5eb6bd440b6bdf6514bb8415", d{"a": "1", "id": "5eb6bd440b6bdf6514bb8415"}},
					{"5eb6bd440b6bdf6514bb8442", d{"b": "2", "id": "5eb6bd440b6bdf6514bb8442"}}},
			},
		},
		// TODO: Include DBs, collections, fields
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			seed(ctx, t, tc.seed, mongoClient)
			defer reset(ctx, t, mongoClient, elasticClient)

			fatalIfErr(t, ioutil.WriteFile(testConfigPath, []byte(tc.config), 0644))

			// Set CLI arguments
			os.Args = append(os.Args, "--config", testConfigPath)

			r, w, err := os.Pipe()
			fatalIfErr(t, err)

			oldStdout := os.Stdout
			os.Stdout = w

			go main()

			s := bufio.NewScanner(r)
			for s.Scan() {
				text := s.Text()
				// Write to original stdout
				_, _ = fmt.Fprintln(oldStdout, text)
				if strings.Contains(text, syncer.MsgDumpingCompleted) {
					break
				}
			}

			os.Stdout = oldStdout

			for idxName, docs := range tc.result {
				for _, doc := range docs {
					resp, err := elasticClient.Get().Index(idxName).Id(doc._id).Do(ctx)
					fatalIfErr(t, err)

					b, _ := resp.Source.MarshalJSON()
					var src map[string]interface{}
					fatalIfErr(t, json.Unmarshal(b, &src))

					if !reflect.DeepEqual(doc.source, src) {
						t.Errorf("Expected document source to be %+v, got %+v (index [%s])", doc.source, src, idxName)
					}
				}
			}
		})
	}
}

func reset(ctx context.Context, t *testing.T, mongoClient *mongo.Client, elasticClient *elastic.Client) {
	dbs, err := mongoClient.ListDatabases(ctx, bson.D{})
	fatalIfErr(t, err)
	for _, db := range dbs.Databases {
		if db.Name != "admin" && db.Name != "config" && db.Name != "local" { // ignore default mongo dbs
			fatalIfErr(t, mongoClient.Database(db.Name).Drop(ctx))
		}
	}

	indices, err := elasticClient.CatIndices().Do(ctx)
	fatalIfErr(t, err)
	for _, index := range indices {
		_, err = elasticClient.DeleteIndex(index.Index).Do(ctx)
		fatalIfErr(t, err)
	}
}

func seed(ctx context.Context, t *testing.T, seed dbSeed, mongoClient *mongo.Client) {
	for dbName, db := range seed {
		for collName, coll := range db {
			_, err := mongoClient.Database(dbName).Collection(collName).InsertMany(ctx, coll)
			fatalIfErr(t, err)
		}
	}
}

func fatalIfErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func getEnvOrDefault(key string, def string) string {
	if e := os.Getenv(key); e != "" {
		return e
	}
	return def
}

func oid(s string) primitive.ObjectID {
	id, _ := primitive.ObjectIDFromHex(s)
	return id
}
