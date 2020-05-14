package mongo

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// changeStreamEventOperationType describes the type of change stream operation.
type changeStreamEventOperationType string

const (
	// ChangeStreamEventOperationTypeInsert describes an insert operation
	ChangeStreamEventOperationTypeInsert changeStreamEventOperationType = "insert"
	// ChangeStreamEventOperationTypeReplace describes an update operation
	ChangeStreamEventOperationTypeReplace changeStreamEventOperationType = "replace"
	// ChangeStreamEventOperationTypeDelete describes a delete operation
	ChangeStreamEventOperationTypeDelete changeStreamEventOperationType = "delete"
	// ChangeStreamEventOperationTypeUpdate describes an update operation
	ChangeStreamEventOperationTypeUpdate changeStreamEventOperationType = "update"
)

// ChangeStreamEvent is a Mongo change stream response document.
// https://docs.mongodb.com/manual/reference/change-events/#change-stream-output
type ChangeStreamEvent struct {
	ID struct {
		Data string `bson:"_data"`
	} `bson:"_id"`
	ClusterTime time.Time `bson:"clusterTime"`
	DocumentKey struct {
		ID primitive.ObjectID `bson:"_id"`
	} `bson:"documentKey"`
	FullDocument map[string]interface{} `bson:"fullDocument"`
	Namespace    struct {
		Collection string `bson:"coll"`
		Database   string `bson:"db"`
	} `bson:"ns"`
	OperationType changeStreamEventOperationType `bson:"operationType"`
}
