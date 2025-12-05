package edu.man.mongo.api.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "brand")
@ToString(exclude = {"brandServiceStation","brandTelephone","brandEmail"})
public class BrandDTO {
    @Id
    String id;
    @Field("name")
    String brandName;
    @Field("type")
    String brandType;
    @Field("origin")
    String brandOrigin;
    @Field("service")
    String brandServiceStation;
    @Field("tel")
    String brandTelephone;
    @Field("email")
    String brandEmail;
}
