package com.csfrez.es.model;


import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName = "users")
@Data
public class User {

    @Id
    private Long id;

    @Field(type = FieldType.Text, name = "name", analyzer = "ik_smart")
    private String name;

    @Field(type = FieldType.Text, name = "sex")
    private String sex;

    @Field(type = FieldType.Integer, name = "age")
    private Integer age;

    @Field(type = FieldType.Text, name = "name", analyzer = "ik_max_word")
    private String desc;

}
