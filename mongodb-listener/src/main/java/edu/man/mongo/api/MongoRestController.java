package edu.man.mongo.api;

import edu.man.mongo.api.dto.BrandDTO;
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
    @GetMapping(value = "/", produces = MediaType.TEXT_PLAIN_VALUE)
    public String landingPage(){
        log.info("Inside landing page");
        return "Successfully loaded into mongodb-listener";
    }
}
