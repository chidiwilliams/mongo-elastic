# mongo-elastic

Stream Mongo to Elasticsearch. mongo-elastic syncs data from MongoDB to Elasticsearch and then tails the MongoDB oplog.

## Target

- MongoDB 4.x

- Elasticsearch 6.x

## TODO

- [x] Parse config

- [x] Connect to Mongo
    - What happens if database, collection, field on config doesn't exist? Ignore

- [x] Connect to Elastic

- [ ] Initial Mongo dump (how to determine where to start dump?)

- [ ] Tail oplog
