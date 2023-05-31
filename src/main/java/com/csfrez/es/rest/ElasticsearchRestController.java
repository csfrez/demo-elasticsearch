package com.csfrez.es.rest;

import co.elastic.clients.elasticsearch.core.search.Hit;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.TypeReference;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        return new ResponseEntity<>("新增失败，数据有误", HttpStatus.OK);
    }

    @PostMapping("/doc/batch/{indexName}")
    public ResponseEntity<Object> batchAddDocument(@PathVariable String indexName, @RequestBody String json) throws IOException {
        if(StringUtils.hasText(json) && json.startsWith("[")){
            //List<Object> objectList = JSONArray.parseArray(json, Object.class);
            List<Map<String, Object>> documentList = JSONObject.parseObject(json, new TypeReference<List<Map<String, Object>>>() {
            });
            return new ResponseEntity<>(elasticsearchService.batchAddDocument(indexName, documentList), HttpStatus.OK);
        }
        return new ResponseEntity<>("新增失败，数据有误", HttpStatus.OK);
    }

    @GetMapping("/doc/{indexName}/{id}")
    public ResponseEntity<Object> getDocument(@PathVariable String indexName, @PathVariable String id) throws IOException {
        if(elasticsearchService.indexIsExist(indexName)){
            return new ResponseEntity<>(elasticsearchService.getDocument(indexName, id, Object.class), HttpStatus.OK);
        }
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PostMapping("/doc/{indexName}/{id}")
    public ResponseEntity<Object> updateDocument(@PathVariable String indexName, @PathVariable String id, @RequestBody String json) throws IOException {
        if(elasticsearchService.indexIsExist(indexName)){
            if(StringUtils.hasText(json) && json.startsWith("{")) {
                JSONObject jsonObject = JSONObject.parseObject(json);
                return new ResponseEntity<>(elasticsearchService.updateDocument(indexName, id, jsonObject, JSONObject.class), HttpStatus.OK);
            }
        }
        return new ResponseEntity<>("更新失败，数据有误", HttpStatus.OK);
    }

    @DeleteMapping("/doc/{indexName}/{id}")
    public ResponseEntity<Object> deleteDocument(@PathVariable String indexName, @PathVariable String id) throws IOException {
        if(elasticsearchService.indexIsExist(indexName)){
            return new ResponseEntity<>(elasticsearchService.deleteDocument(indexName, id), HttpStatus.OK);
        }
        return new ResponseEntity<>("删除失败，数据有误", HttpStatus.OK);
    }

    @GetMapping("/doc/list/{indexName}")
    public ResponseEntity<Object> searchDocumentList(@PathVariable String indexName, String field, String value) throws IOException {
        if(elasticsearchService.indexIsExist(indexName)){
            List<Hit<JSONObject>> hitList = elasticsearchService.searchDocumentList(indexName, JSONObject.class, field, value);
            log.info("hitList={}", hitList);
            List<JSONObject> jsonObjectList = new ArrayList<>();
            for (Hit<JSONObject> hit: hitList) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(hit.id(), hit.source());
                jsonObjectList.add(jsonObject);
            }
            return new ResponseEntity<>(jsonObjectList, HttpStatus.OK);
        }
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PostMapping("/doc/page/{indexName}")
    public ResponseEntity<Object> searchDocumentPage(@PathVariable String indexName, @RequestBody String json,
                                                     @RequestParam(defaultValue = "1") Integer pageNo,
                                                     @RequestParam(defaultValue = "2") Integer pageSize) throws IOException {
        List<Map<String, String>> paramList = new ArrayList<>();
        if(StringUtils.hasText(json) && json.startsWith("[")) {
            paramList = JSONObject.parseObject(json, new TypeReference<List<Map<String, String>>>() {
            });
        }
        if(elasticsearchService.indexIsExist(indexName)){
            List<Hit<JSONObject>> hitList = elasticsearchService.searchDocumentPage(indexName, JSONObject.class, paramList, pageNo, pageSize);
            log.info("hitList={}", hitList);
            List<JSONObject> jsonObjectList = new ArrayList<>();
            for (Hit<JSONObject> hit: hitList) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(hit.id(), hit.source());
                jsonObjectList.add(jsonObject);
            }
            return new ResponseEntity<>(jsonObjectList, HttpStatus.OK);
        }
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
