package com.csfrez.es.mapper;

import com.csfrez.es.entity.FaqPair;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Author: csfrez
 * @CreateDate: 2023/05/30
 * @Description Mapper映射
 */
@Mapper
public interface FaqPairMapper {

    @Select("select id,qa_id,standard_question,standard_answer from faq_pair")
    @Results({
            @Result(property = "id", column = "id", id = true),
            @Result(property = "qaId", column = "qa_id"),
            @Result(property = "standardQuestion", column = "standard_question"),
            @Result(property = "standardAnswer", column = "standard_answer")
    })
    List<FaqPair> selectAll();

    @Results({
            @Result(property = "id", column = "id", id = true),
            @Result(property = "qaId", column = "qa_id"),
            @Result(property = "standardQuestion", column = "standard_question"),
            @Result(property = "standardAnswer", column = "standard_answer")
    })
    @Select("select id,qa_id,standard_question,standard_answer from faq_pair where id =#{id}")
    FaqPair selectFaqPairById(Integer id);
}
