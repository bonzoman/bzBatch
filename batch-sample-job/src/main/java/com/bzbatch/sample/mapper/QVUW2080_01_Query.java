package com.bzbatch.sample.mapper;

import lombok.Builder;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface QVUW2080_01_Query {
    @Select("""
            SELECT
             LOB_CD as lobCd
            , ITEM_NAME as itemName
            , SEQ_NO AS seqNo
            , ITEM_ATTR01 AS itemAttr01
            , ITEM_ATTR02 AS itemAttr02
            , ITEM_ATTR03 AS itemAttr03
            , ITEM_ATTR04 AS itemAttr04
            , ITEM_ATTR05 AS itemAttr05
            FROM AU01
            WHERE 1=1
            AND LOB_CD = #{lobCd}
            """)
    List<SamgSrchResVo> select2080_01(String lobCd);

    @Builder
    record SamgSrchResVo(
            String lobCd,
            String itemName,
            int seqNo,
            String itemAttr01,
            String itemAttr02,
            String itemAttr03,
            String itemAttr04,
            int itemAttr05
    ) {
    }
}
