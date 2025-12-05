package edu.man.mongo.api.service;

import edu.man.mongo.api.dto.BrandDTO;
import edu.man.mongo.api.repo.BrandRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class BrandService {
    @Autowired
    private BrandRepository brandRepository;

    private static String EXCLUDED_BRAND = "nokia";


    public Flux<List<BrandDTO>> streamAllBrands(int batchCount) {
        log.info("Inside streamAllBrands - Starting stream of all brands from MongoDB in batches of {}.",batchCount);
        AtomicLong start = new AtomicLong(System.currentTimeMillis());
        AtomicLong end = new AtomicLong(System.currentTimeMillis());

        Flux<BrandDTO> allBrands = brandRepository.findAll()
                .doOnEach(signal -> {
                    if (signal.isOnSubscribe()) {
                        start.set(System.currentTimeMillis());
                    }
                    if (signal.isOnError() || signal.isOnComplete()) {
                        end.set(System.currentTimeMillis());
                    }
                    log.info("Processing {} - elapsed time is {}",signal.get(),(end.get()-start.get()));
                })
                .delayElements(Duration.of(175, ChronoUnit.MILLIS))
                .doOnComplete(() -> log.info("Finished Fetching all records from Brand collection"))
                .doOnCancel(() -> log.info("Cancelled record request from Brand collection"))
                .doOnError(e -> log.info("Exception while processing data from Brand collection"));

        // 2. Use handle() to implement filtering (skipping the EXCLUDED_BRAND) and logging.
        Flux<BrandDTO> filteredBrands = allBrands
                .handle((brand, sink) -> {
                    if (!brand.getBrandName().toLowerCase().contains(EXCLUDED_BRAND)) {
                        sink.next(brand); // Keep the item
                    } else {
                        log.debug("Skipping brand via handle(): {}", brand.getBrandName());
                        // No call to sink.next(), so the item is dropped
                    }
                });

        // 3. Use the buffer(3) operator to group the filtered elements.
        Flux<List<BrandDTO>> bufferedBrands = filteredBrands.buffer(3);
        // 4. Add a ticking interval to control the emission rate of the batches.
        Flux<Long> ticker = Flux.interval(Duration.ofSeconds(1));

        // 5. Combine the buffered stream with the ticker stream.
        return Flux.zip(bufferedBrands, ticker)
                .map(tuple -> tuple.getT1()) // Return only the List<Brand> (T1)
                .doOnComplete(() -> log.info("Completed streaming all brand batches."));
    }
}
