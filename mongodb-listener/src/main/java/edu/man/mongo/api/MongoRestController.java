package edu.man.mongo.api;

import edu.man.mongo.api.dto.BrandDTO;
import edu.man.mongo.api.repo.BrandRepository;
import edu.man.mongo.api.service.BrandService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/mongo-listener/api")
public class MongoRestController {
    @Autowired
    private BrandService brandService;

    @GetMapping(value = "/", produces = MediaType.TEXT_PLAIN_VALUE)
    public String landingPage(){
        log.info("Inside landing page");
        return "Successfully loaded into mongodb-listener";
    }

    @GetMapping(value = "/stream/brand", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<List<BrandDTO>> streamBatchedBrands() {
        log.info("Request received to stream batched brands (3 rows per event).");
        // Delegate the batched streaming logic to the service
        return brandService.streamAllBrands(3);
    }
}
