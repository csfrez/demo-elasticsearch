package com.csfrez.es.service;

import com.csfrez.es.entity.FaqPair;
import com.csfrez.es.mapper.FaqPairMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FaqPairService {

    @Autowired
    private FaqPairMapper faqPairMapper;

    public List<FaqPair> getAllFagPair(){
        return faqPairMapper.selectAll();
    }

    public FaqPair getFaqPariById(Integer id){
        return faqPairMapper.selectFaqPairById(id);
    }

}
