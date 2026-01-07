package edu.man.kafka.processor;

import edu.man.pojo.Brand;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

import java.util.Locale;

@Slf4j
public class BrandProcessor implements Processor<String,String,String, Brand> {

    private ProcessorContext<String, Brand> processorContext;
    @Override
    public void init(ProcessorContext<String, Brand> context) {
        log.info("Inside BrandProcessor initialization");
        Processor.super.init(context);
        this.processorContext = context;

    }

    @Override
    public void process(Record<String, String> inRecord) {
        log.info("Inside BrandProcessor process key is {} and value is {}", inRecord.key(), inRecord.value());
        final RecordMetadata recordMetadata = this.processorContext.recordMetadata().get();
        log.info(
                "Task id {} :: Processing from Topic: {} in partition: {} :: offset: {}",
                this.processorContext.taskId(),
                recordMetadata.topic(),
                recordMetadata.partition(),
                recordMetadata.offset());
        if(inRecord.key().toLowerCase().equals("brand")){
            String[] bParams = inRecord.value().toLowerCase().split(",");
            if(bParams.length > 0) {
                String brandName = bParams.length > 0 ? bParams[0] : "";
                String brandColor = bParams.length > 1? bParams[1]: "NIL";
                Brand brand = new Brand(recordMetadata.offset(), brandName, brandColor);
                final Headers kafkaHeaders = new RecordHeaders();
                kafkaHeaders.add("header-timestamp", new String(System.currentTimeMillis()+"").getBytes());
                Record<String, Brand> outRecord = new Record<>(inRecord.key().toLowerCase(),
                        brand, System.currentTimeMillis(),kafkaHeaders);
                this.processorContext.forward(outRecord, "brand-sink-node");
            }else{
                log.info("No value in topic message");
            }
        }else{
            log.info("No brand in topic message");
        }
    }
}
