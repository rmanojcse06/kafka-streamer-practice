package edu.man.mongo.listener.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.model.changestream.OperationType;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonParseException;
import org.bson.json.JsonWriterSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ChangeStreamMongoListener {
    private final JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED)
            .build();
    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;
    @Autowired
    private KafkaTemplate kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Value("${app.mongodb.collection}")
    private String collectionName;
    @Value("${app.kafka.topic}")
    private String kafkaTopic;
    private String databaseName;

    @PostConstruct
    public void bootstrapListener() {

        /*
        ChangeStreamOptions options = ChangeStreamOptions.builder()
                .filter(
                        new Document("$match",
                                new Document("operationType",
                                        new Document("$in",
                                                java.util.Arrays.asList("insert", "update", "replace", "delete")
                                        )
                                )
                        )
                )
                .build();

                Flux<ChangeStreamEvent<Document>> changeStream = reactiveMongoTemplate.changeStream(
                       "inventorydb", "inventory", options, Document.class );
        */


        reactiveMongoTemplate.getMongoDatabase()
                .map(com.mongodb.reactivestreams.client.MongoDatabase::getName)
                .subscribe(dbName -> {
                    this.databaseName = dbName;
                    log.info("The connected database name is: {}", dbName);
                });

        ChangeStreamOptions options = ChangeStreamOptions.builder()
                .returnFullDocumentOnUpdate()
                .build();

        reactiveMongoTemplate.changeStream("rawsrcdb", "rawdata", options, Document.class)
                .doOnError(err -> log.error("Change stream error", err))
                .retry()  // <--- IMPORTANT: automatic restart
                .subscribe(event -> {
                    OperationType operation = event.getOperationType();
                    String kafkaKey = operation.getValue();
                    Document body = event.getBody();
                    log.info("BSON operation is {} and body is {}", operation, null != body ? body : "");
                    if (body != null && body.containsKey("table")) {
                        kafkaKey = kafkaKey+"__"+body.get("table", "None").toString();
                        try {
                            ObjectNode rootNode = this.objectMapper.createObjectNode();
                            rootNode.put("operation", operation.getValue());
                            rootNode.put("key", kafkaKey);
                            Object payloadBson = body.get("payload");
                            if (payloadBson instanceof Document) {
                                try {
                                    rootNode.put("payload", ((Document) payloadBson).toJson(jsonWriterSettings));
                                }catch(JsonParseException jpe){
                                    rootNode.put("payload", "json-parse-exception");
                                }
                            }
                            String jsonPayload = this.objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);

                            log.info("MongoDB key: {} | Operation: {}", kafkaKey, event.getOperationType());
                            log.info("Pushing to Kafka with key {} and payload {}", kafkaKey, jsonPayload);

                            // Produce to Kafka
                            kafkaTemplate.send(this.kafkaTopic, kafkaKey, jsonPayload)
                                    .whenComplete((result, ex) -> {
                                        if (ex != null) {
                                            log.error("Kafka Send Failed", ex);
                                        }
                                    });
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        log.info("Skipping this message as there is no table field added in RAWDATA");
                        return;
                    }
                });
        log.info("✔ MongoDB change stream listener registered for inventory collection");
    }
}
