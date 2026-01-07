package edu.man.kafka.processor;

import edu.man.pojo.Brand;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Set;

public class BrandProcessorSupplier implements ProcessorSupplier<String,String,String, Brand> {

    @Override
    public Processor<String, String, String, Brand> get() {
        return new BrandProcessor();
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return ProcessorSupplier.super.stores();
    }
}
