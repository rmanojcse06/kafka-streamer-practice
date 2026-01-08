package edu.man.kafka.streamer;

import edu.man.kafka.processor.BrandProcessorSupplier;
import edu.man.pojo.Brand;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class RealStreamingApp {
    public static void main(String[] args) {
        log.info("Starting RealStreamingApp");
        final StreamsBuilder builder = new StreamsBuilder();
        final Topology topology = _makeTopology(builder);
        startKafkaStreams(topology);
    }


    static protected Topology _makeTopology(StreamsBuilder streamsBuilder) {
        Topology t = streamsBuilder.build();
        JsonSerializer<Brand> serializer = new JsonSerializer<>();
        JsonDeserializer<Brand> deserializer = new JsonDeserializer<>(Brand.class);
        deserializer.addTrustedPackages("*"); // Allow deserialization from any package

        Serde<Brand> brandSerde = Serdes.serdeFrom(serializer, deserializer);
        t.addSource("init-source-node",
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer(),
                "man.src.raw.data");
        t.addProcessor("process-1",
                new BrandProcessorSupplier(),
                "init-source-node");
        StoreBuilder<KeyValueStore<String, Long>> brandCountStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("brand-counts-store"),
                Serdes.String(),
                Serdes.Long()
        );
        t.addStateStore(brandCountStoreBuilder, "process-1");
        t.addSink("brand-sink-node",
                "man.sink.brand.data",
                Serdes.String().serializer(),
                brandSerde.serializer(),
                "process-1");
        return t;
    }

    static protected Properties _makeKafkaProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dag-real-streaming-app-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");
        return props;
    }

    static protected void startKafkaStreams(Topology topology){
        KafkaStreams kafkaStreams = new KafkaStreams(topology,_makeKafkaProperties());
        final CountDownLatch latch = new CountDownLatch(1);
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("real-streaming-app-1-shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        try {
            log.info("Starting Kafka Streams - man.src.raw.data to man.sink.brand.data");
            log.info("Start producing Kafka Streams as follows . . . ");
            log.info("docker-compose -f docker-compose.yaml exec kstream-infra-kafka bash\n" +
                    "kafka-console-producer --bootstrap-server localhost:29092 --topic man.src.raw.data --property parse.key=true --property key.separator=-\n" +
                    "> brand-samsung,BLUE\n" +
                    "> brand-sony,yellow");
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}