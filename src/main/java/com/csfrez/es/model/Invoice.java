package com.csfrez.es.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import lombok.Data;

@Document(indexName = "invoices")
@Data
public class Invoice {

    @Id
    private String id;

    @Field(type = FieldType.Text, name = "name", analyzer = "ik_smart")
    private String name;

    @Field(type = FieldType.Text, name = "number")
    private String number;

    @Field(type = FieldType.Double, name = "amount")
    private Double amount;
}