# mongo-elastic-sync

[![Build status](https://github.com/chidiwilliams/mongo-elastic-sync/workflows/Build/badge.svg)](https://github.com/chidiwilliams/mongo-elastic-sync/actions?query=workflow%3ABuild) [![codecov](https://codecov.io/gh/chidiwilliams/mongo-elastic-sync/branch/master/graph/badge.svg)](https://codecov.io/gh/chidiwilliams/mongo-elastic-sync)

Stream Mongo to Elasticsearch. mongo-elastic-sync syncs data from MongoDB to Elasticsearch and then tails the MongoDB change stream.

## Target

- MongoDB 4.x

- Elasticsearch 6.x

## TODO

- [x] Parse config

- [x] Connect to Mongo

- [x] Connect to Elastic

- [x] Initial Mongo dump

- [x] Tail change stream

  - [ ] Resume change stream progress