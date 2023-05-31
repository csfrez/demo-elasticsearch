package com.csfrez.es.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class ElasticsearchService {

    @Autowired
    private ElasticsearchClient elasticsearchClient;

    /**
     * 创建索引
     *
     * @param indexName
     * @throws IOException
     */
    public boolean createIndex(String indexName) throws IOException {
        CreateIndexResponse createIndexResponse = elasticsearchClient.indices()
                .create(createIndexRequest ->
                        createIndexRequest.index(indexName)
                );
        log.info("索引{}创建是否成功: {}", indexName, createIndexResponse.acknowledged());
        return createIndexResponse.acknowledged();
    }

    /**
     * 创建索引 - 指定 mapping
     *
     * @param indexName
     * @param typeMapping
     * @throws IOException
     */
    public boolean createIndex(String indexName, TypeMapping typeMapping) throws IOException {
        CreateIndexResponse createIndexResponse = elasticsearchClient.indices()
                .create(createIndexRequest ->
                        createIndexRequest.index(indexName).mappings(typeMapping)
                );
        log.info("索引{}创建是否成功: {}", indexName, createIndexResponse.acknowledged());
        return createIndexResponse.acknowledged();
    }

    /**
     * 创建索引 - 指定json流
     *
     * @param indexName
     * @param inputStream
     * @throws IOException
     */
    public boolean createIndex(String indexName, InputStream inputStream) throws IOException {
        CreateIndexResponse createIndexResponse = elasticsearchClient.indices()
                .create(createIndexRequest ->
                        createIndexRequest.index(indexName).withJson(inputStream)
                );
        log.info("索引{}创建是否成功: {}", indexName, createIndexResponse.acknowledged());
        return createIndexResponse.acknowledged();
    }

    /**
     * 查询索引是否存在
     *
     * @param indexName
     * @throws IOException
     */
    public boolean indexIsExist(String indexName) throws IOException {
        BooleanResponse booleanResponse = elasticsearchClient.indices()
                .exists(existsRequest ->
                        existsRequest.index(indexName)
                );
        log.info("索引{}是否存在: {}", indexName, booleanResponse.value());
        return booleanResponse.value();
    }

    /**
     * 查看索引的相关信息
     *
     * @throws IOException
     */
    public Map<String, Property> indexDetail(String indexName) throws IOException {
        GetIndexResponse getIndexResponse = elasticsearchClient.indices()
                .get(getIndexRequest ->
                        getIndexRequest.index(indexName)
                );
        Map<String, Property> properties = getIndexResponse.get(indexName).mappings().properties();
        for (String key : properties.keySet()) {
            log.info("索引{}的详细信息为key: {}, Property: {}", indexName, key, properties.get(key)._kind());
        }
        return properties;
    }

    /**
     * 删除索引
     *
     * @throws IOException
     */
    public boolean deleteIndex(String indexName) throws IOException {
        DeleteIndexResponse deleteIndexResponse = elasticsearchClient.indices()
                .delete(deleteIndexRequest ->
                        deleteIndexRequest.index(indexName)
                );
        log.info("索引{}是否删除成功: {}", indexName, deleteIndexResponse.acknowledged());
        return deleteIndexResponse.acknowledged();
    }

    /**
     * 添加文档
     * @param indexName
     * @param id
     * @param document
     * @return
     * @throws IOException
     */
    public Result addDocument(String indexName, String id, Object document) throws IOException {
        IndexResponse indexResponse = elasticsearchClient.index(indexRequest ->
                indexRequest.index(indexName).id(id).document(document)
        );
        log.info("response: {}, responseStatus: {}", indexResponse, indexResponse.result());
        return indexResponse.result();
    }

    /**
     * 获取文档信息
     * @param indexName
     * @param id
     * @param clazz
     * @param <T>
     * @return
     * @throws IOException
     */
    public <T> T getDocument(String indexName, String id, Class<T> clazz) throws IOException {
        GetResponse<T> getResponse = elasticsearchClient.get(getRequest ->
                getRequest.index(indexName).id(id), clazz
        );
        log.info("getResponse: {}, source: {}", getResponse, getResponse.source());
        return getResponse.source();
    }

    /**
     * 更新文档信息
     * @param indexName
     * @param id
     * @param document
     * @param clazz
     * @return
     * @throws IOException
     */
    public Result updateDocument(String indexName, String id, Object document, Class<?> clazz) throws IOException {
        UpdateResponse<?> updateResponse = elasticsearchClient.update(updateRequest ->
                updateRequest.index(indexName).id(id).doc(document), clazz
        );
        log.info("updateResponse: {}, responseStatus: {}", updateResponse, updateResponse.result());
        return updateResponse.result();
    }

    /**
     * 删除文档信息
     * @param indexName
     * @param id
     * @return
     * @throws IOException
     */
    public Result deleteDocument(String indexName, String id) throws IOException {
        DeleteResponse deleteResponse = elasticsearchClient.delete(deleteRequest ->
                deleteRequest.index(indexName).id(id)
        );
        log.info("deleteResponse: {}, result:{}", deleteResponse, deleteResponse.result());
        return deleteResponse.result();
    }

    /**
     * 批量插入文档
     * @param indexName
     * @param documentList
     * @return
     * @throws IOException
     */
    public boolean batchAddDocument(String indexName, List<Map<String, Object>> documentList) throws IOException {
        List<BulkOperation> bulkOperationList = new ArrayList<>();
        for(Map<String, Object> document: documentList){
            document.forEach((key, value) -> {
                bulkOperationList.add(new BulkOperation.Builder().create(e -> e.id(key).document(value)).build());
            });
        }
        BulkResponse bulkResponse = elasticsearchClient.bulk(bulkRequest ->
                bulkRequest.index(indexName).operations(bulkOperationList)
        );
        log.info("bulkResponse: {}, errors:{}", bulkResponse, bulkResponse.errors());
        return !bulkResponse.errors();
    }


    public <T> List<Hit<T>> searchDocumentList(String indexName, Class<T> clazz, String field, String value) throws IOException {
        SearchResponse<T> search = elasticsearchClient.search(s -> {
            if(StringUtils.hasText(field) && StringUtils.hasText(value)){
                //s.query(q -> q.term(t -> t.field(field).value(v -> v.stringValue(value))));
                s.query(q -> q.match(t -> t.field(field).query(value)));
            }
            return s.index(indexName);
        }, clazz);
        log.info("search={}", search);
        return search.hits().hits();
    }

    public <T> List<Hit<T>> searchDocumentPage(String indexName, Class<T> clazz, List<Map<String, String>> paramList,
                                               Integer pageNo, Integer pageSize) throws IOException {
        int from = (pageNo-1) * pageSize;
        SearchResponse<T> search = elasticsearchClient.search(s -> s.index(indexName).from(from).size(pageSize)
                        .query(query -> query.bool(boolQuery -> {
                            for(Map<String, String> paramMap: paramList){
                                for(Map.Entry<String, String> entry : paramMap.entrySet()){
                                    //boolQuery.must(must -> must.term(e -> e.field(entry.getKey()).value(entry.getValue())));
                                    boolQuery.must(must -> must.match(e -> e.field(entry.getKey()).query(entry.getValue())));
                                }
                            }
                            return boolQuery;
                        })),
                clazz);
        log.info("search={}", search);
        return search.hits().hits();
    }

}
