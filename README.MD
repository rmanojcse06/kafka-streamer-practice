# Kafka Streamer Practice
This is a practice project to stream raw data into mongodb

### Step(1): Start mongo infra
```bash
docker-compose -f docker-compose.yaml up -d kstream-infra-mongo 
docker-compose -f docker-compose.yaml exec kstream-infra-mongo mongosh "mongodb://inventorydba:password@localhost:27017/rawsrcdb" --authenticationDatabase admin
```

```bash
show collections;
db.rawdata.find();

```

### Step(2)