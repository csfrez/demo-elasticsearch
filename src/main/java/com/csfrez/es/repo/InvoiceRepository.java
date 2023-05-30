package com.csfrez.es.repo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.csfrez.es.model.Invoice;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Repository
public class InvoiceRepository {

    @Autowired
    private ElasticsearchClient elasticsearchClient;

    private final String indexName = "invoices";

    public String createOrUpdateInvoice(Invoice invoice) throws IOException {
        IndexResponse response = elasticsearchClient.index(
                i -> i.index(indexName)
                        .id(invoice.getId())
                        .document(invoice)
        );
        if(response.result().name().equals("Created")){
            return new StringBuilder("Invoice document has been created successfully.").toString();
        }else if(response.result().name().equals("Updated")){
            return new StringBuilder("Invoice document has been updated successfully.").toString();
        }
        return new StringBuilder("Error while performing the operation.").toString();
    }

    public Invoice getInvoiceById(String invoiceId) throws IOException{
        Invoice invoice = null;
        GetResponse<Invoice> response = elasticsearchClient.get(
                g -> g.index(indexName)
                        .id(invoiceId),
                Invoice.class
        );

        if (response.found()) {
            invoice = response.source();
            System.out.println("Invoice name is: " + invoice.getName());
        } else {
            System.out.println ("Invoice not found");
        }
        return invoice;
    }

    public String deleteInvoiceById(String invoiceId) throws IOException {

        DeleteRequest request = DeleteRequest.of(d -> d.index(indexName).id(invoiceId));

        DeleteResponse deleteResponse = elasticsearchClient.delete(request);
        if (Objects.nonNull(deleteResponse.result()) && !deleteResponse.result().name().equals("NotFound")) {
            return new StringBuilder("Invoice with id : " + deleteResponse.id()+ " has been deleted successfully !.").toString();
        }
        System.out.println("Invoice not found");
        return new StringBuilder("Invoice with id : " + deleteResponse.id()+" does not exist.").toString();
    }

    public List<Invoice> getAllInvoices() throws IOException {
        SearchRequest searchRequest = SearchRequest.of(s -> s.index(indexName));
        SearchResponse<Invoice> searchResponse = elasticsearchClient.search(searchRequest, Invoice.class);
        List<Hit<Invoice>> hits = searchResponse.hits().hits();
        List<Invoice> invoices = new ArrayList<>();
        for(Hit<Invoice> object : hits){
            System.out.print(object.source());
            invoices.add(object.source());
        }
        return invoices;
    }
}