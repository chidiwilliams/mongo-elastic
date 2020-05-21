docker-compose -f docker-compose.test.yml exec -T mongo1 mongo --eval "rs.initiate(
  {
    _id : 'rs0',
    members: [
      { _id : 0, host : 'mongo1:27017' },
      { _id : 1, host : 'mongo2:27017' },
      { _id : 2, host : 'mongo3:27017' }
    ]
  }
)"

#sudo echo "127.0.0.1 mongo1
#127.0.0.1 mongo2
#127.0.0.1 mongo3" | sudo tee -a /etc/hosts
