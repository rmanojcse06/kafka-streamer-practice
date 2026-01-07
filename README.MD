# Kafka Streamer Practice
This is a practice project to stream raw data into mongodb

### Step(1): Start mongo infra
```bash
docker-compose -f docker-compose.yaml up -d kstream-infra-mongo 
docker-compose -f docker-compose.yaml exec kstream-infra-mongo bash 
  mongosh "mongodb://inventorydba:password@localhost:27017/rawsrcdb" --authenticationDatabase admin
```
```bash
cd dag-kafka-streamer-p2
#mvn spring-boot:run -Dstart-class=edu.man.kafka.streamer.SimpleFirstStreamingApp
mvn exec:java -Dexec.mainClass=edu.man.kafka.streamer.RealStreamingApp
mvn dependency:tree
docker-compose -f ../docker-compose.yaml exec kstream-infra-kafka bash
kafka-console-consumer --bootstrap-server localhost:29092 --topic man.sink.brand.data

docker-compose -f ../docker-compose.yaml exec kstream-infra-kafka bash
kafka-topics --bootstrap-server localhost:29092 --create --topic man.src.raw.data --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:29092 --list
kafka-console-producer --bootstrap-server localhost:29092 --topic man.src.raw.data --property parse.key=true --property key.separator=-
```










```bash
cd simple-kafka-streamer-p1
#mvn spring-boot:run -Dstart-class=edu.man.kafka.streamer.SimpleFirstStreamingApp
mvn exec:java -Dexec.mainClass=edu.man.kafka.streamer.SimpleFirstStreamingApp
mvn dependency:tree
docker-compose -f docker-compose.yaml exec kstream-infra-kafka bash
kafka-console-consumer --bootstrap-server localhost:29092 --topic man.simple.first.red.sink.topic

kafka-topics --create --bootstrap-server localhost:29092 --replication-factor 1 --partitions 1 --topic man.simple.first.src.topic
kafka-topics --create --bootstrap-server localhost:29092 --replication-factor 1 --partitions 1 --topic man.simple.first.red.sink.topic
kafka-topics --bootstrap-server localhost:29092 --list
```
```bash
docker-compose -f docker-compose.yaml exec kstream-infra-kafka bash
kafka-console-producer --bootstrap-server localhost:29092 --topic man.simple.first.src.topic --property parse.key=true --property key.separator=-
```
```bash
> green-goblin
> red-daisy 
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