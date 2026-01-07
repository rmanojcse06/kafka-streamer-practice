# Kafka Streamer Practice
This is a practice project to stream raw data into mongodb

### Step(1): Start mongo infra
```bash
docker-compose -f docker-compose.yaml up -d kstream-infra-mongo 
docker-compose -f docker-compose.yaml exec kstream-infra-mongo bash 
  mongosh "mongodb://inventorydba:password@localhost:27017/rawsrcdb" --authenticationDatabase admin
```

```bash
cd mongodb-listener
 mvn spring-boot:run -Dstart-class=edu.man.mongo.Main
```

```bash
show collections;
db.rawdata.find();
db.rawdata.countDocuments();
db.rawdata.insertOne({table:"brand",payload:{name:"samsung"}});
```

```bash
kafka-topics --list --bootstrap-server localhost:29092
```
man.event.raw.stream

```bash
docker-compose -f docker-compose.yaml exec kstream-infra-kafka bash
kafka-console-consumer --bootstrap-server localhost:29092 --topic man.event.raw.stream --from-beginning
```

### Step(2)