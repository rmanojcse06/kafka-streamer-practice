package edu.man.mongo.api.repo;

import edu.man.mongo.api.dto.BrandDTO;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BrandRepository extends ReactiveMongoRepository<BrandDTO,String> {
}
