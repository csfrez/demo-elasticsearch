package com.csfrez.es.rest;

import com.alibaba.fastjson2.JSONObject;
import com.csfrez.es.service.ElasticsearchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

@RestController
@RequestMapping("/es")
@Slf4j
public class ElasticsearchRestController {

    @Autowired
    private ElasticsearchService elasticsearchService;

    @PostMapping("/index/{indexName}")
    public ResponseEntity<Object> createIndex(@PathVariable String indexName, @RequestBody String json) throws IOException {
        if(elasticsearchService.indexIsExist(indexName)){
            elasticsearchService.deleteIndex(indexName);
        }

        if(StringUtils.hasText(json) && json.startsWith("{")){
            /*JSONObject jsonObject = JSONObject.parseObject(json);
            JSONObject properties = jsonObject.getJSONObject("properties");
            TypeMapping typeMapping = TypeMapping.of(type -> {
                properties.forEach((key, obj) -> {
                    JSONObject subJsonObject = (JSONObject) obj;
                    if ("text".equals(subJsonObject.getString("type"))) {
                        TextProperty textProperty = TextProperty.of(text -> text.fielddata(true)
                                .analyzer(subJsonObject.getString("analyzer"))
                                .searchAnalyzer(subJsonObject.getString("search_analyzer")));
                        type.properties(key, objectBuilder -> objectBuilder.text(textProperty));
                    }
                });
                return type;
            });
            return new ResponseEntity<>(elasticsearchService.createIndex(indexName, typeMapping), HttpStatus.OK);*/
            InputStream inputStream = new ByteArrayInputStream(json.getBytes());
            return new ResponseEntity<>(elasticsearchService.createIndex(indexName, inputStream), HttpStatus.OK);
        }
        return new ResponseEntity<>(elasticsearchService.createIndex(indexName), HttpStatus.OK);
    }

    @GetMapping("/index/{indexName}")
    public ResponseEntity<Object> indexDetail(@PathVariable String indexName) throws IOException {
        if(elasticsearchService.indexIsExist(indexName)){
            return new ResponseEntity<>(elasticsearchService.indexDetail(indexName), HttpStatus.OK);
        }
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @DeleteMapping("/index/{indexName}")
    public ResponseEntity<Object> deleteIndex(@PathVariable String indexName) throws IOException {
        return new ResponseEntity<>(elasticsearchService.deleteIndex(indexName), HttpStatus.OK);
    }

    @PostMapping("/doc/{indexName}")
    public ResponseEntity<Object> addDocument(@PathVariable String indexName, @RequestBody String json) throws IOException {
        String id = System.currentTimeMillis() + "";
        if(StringUtils.hasText(json) && json.startsWith("{")){
            JSONObject jsonObject = JSONObject.parseObject(json);
            return new ResponseEntity<>(elasticsearchService.addDocument(indexName, id, jsonObject), HttpStatus.OK);
        }
        return new ResponseEntity<>(elasticsearchService.addDocument(indexName, id, json), HttpStatus.OK);
    }
}
