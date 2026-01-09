package edu.man.kafka.streamer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class SimpleFirstStreamingApp {
    public static void main(String[] args) {
        log.info("Starting Simple Kafka Stremer Project");
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> myFirstStream = builder.stream("man.simple.first.src.topic",
                                                                Consumed.with(Serdes.String(), Serdes.String()));
        myFirstStream.peek((k,v)->log.info("Stream k is {} and v is {}", k,v))
                .mapValues(v->v.toLowerCase())
                .filter((k,v)->k.contains("red"))
                .selectKey((k,v)->k.toLowerCase())
                .peek((k,v)->log.info("Passing over topic key {} and value {} ",k,v))
                .to("man.simple.first.red.sink.topic",
                        Produced.with(Serdes.String(), Serdes.String()));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-first-streaming-app-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("simple-first-streaming-app-1-shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        try {
            log.info("Starting Kafka Streams - man.simple.first.src.topic to man.simple.first.red.sink.topic");
            log.info("Start producing Kafka Streams as follows . . . ");
            log.info("docker-compose -f docker-compose.yaml exec kstream-infra-kafka bash\n" +
                    "kafka-console-producer --bootstrap-server localhost:29092 --topic man.simple.first.src.topic --property parse.key=true --property key.separator=-\n" +
                    "> green-goblin\n" +
                    "> red-daisy ");
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}