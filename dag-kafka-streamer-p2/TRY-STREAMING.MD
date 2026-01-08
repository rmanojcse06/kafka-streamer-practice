### Step(1): Start kafka infra
```bash
cd dag-kafka-streamer-p2
docker-compose -f ../docker-compose.yaml up
```

### Step(2): Create kafka topic
Login into kafka infra
```bash
docker-compose -f ../docker-compose.yaml exec kstream-infra-kafka bash
kafka-topics --bootstrap-server localhost:29092 --list
```
Create kafka topic
```bash
kafka-topics --bootstrap-server localhost:29092 --create --topic man.src.raw.data --partitions 3 --replication-factor 1
```

### Step(3): Reset kafka consumer
Reset Consumer, ApplicationID will be consumer group name
```bash
kafka-consumer-groups --bootstrap-server localhost:29092 --group dag-real-streaming-app-2 --topic man.sink.brand.data --reset-offsets --to-earliest --execute
```

### Step(4): Monitor the Consumer
```bash
cd dag-kafka-streamer-p2
docker-compose -f ../docker-compose.yaml exec kstream-infra-kafka bash
kafka-console-consumer --bootstrap-server localhost:29092 --topic man.sink.brand.data
```

### Step(5): Monitor the State Store
```bash
cd dag-kafka-streamer-p2
docker-compose -f ../docker-compose.yaml exec kstream-infra-kafka bash
kafka-console-consumer --bootstrap-server localhost:29092 --topic dag-real-streaming-app-2-brand-counts-store-changelog --from-beginning --property print.key=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```
### Step(6): Execute the streamer Application
```bash
cd dag-kafka-streamer-p2
mvn exec:java -Dexec.mainClass=edu.man.kafka.streamer.RealStreamingApp
```

### Step(7): Create Kafka Producer
```bash
cd dag-kafka-streamer-p2
docker-compose -f ../docker-compose.yaml exec kstream-infra-kafka bash
kafka-console-producer --bootstrap-server localhost:29092 --topic man.src.raw.data --property parse.key=true --property key.separator=-
```