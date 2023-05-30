package com.csfrez.es.rest;

import com.csfrez.es.entity.FaqPair;
import com.csfrez.es.service.FaqPairService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/faq")
@Slf4j
public class FaqPairRestController {

    @Autowired
    private FaqPairService faqPairService;

    @GetMapping("/all")
    public ResponseEntity<Object> all() {
        List<FaqPair> faqPairList = faqPairService.getAllFagPair();
        log.info("faqPairList={}", faqPairList);
        return new ResponseEntity<>(faqPairList, HttpStatus.OK);
    }

    @GetMapping("/detail/{id}")
    public ResponseEntity<Object> detail(@PathVariable Integer id) {
        FaqPair faqPair = faqPairService.getFaqPariById(id);
        log.info("faqPair={}", faqPair);
        return new ResponseEntity<>(faqPair, HttpStatus.OK);
    }
}
