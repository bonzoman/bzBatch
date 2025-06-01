package com.bzbatch.sampleChunk.mapper;

import com.bzbatch.sampleChunk.dto.InFileAu02Vo;
import lombok.Builder;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface QVUW_Query {

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

    @Select("""
            SELECT ENAME
              FROM EMP
             WHERE JOB =#{job}
            """)
    String selectManager(String job);

    int insert2080_01(InFileAu02Vo inFileAu02Vo);

    int insert2080_02(InFileAu02Vo inFileAu02Vo);
    
    int insert2080_03List(InFileAu02Vo inFileAu02Vo);

    @Delete("DELETE FROM AU02 WHERE LOB_CD = #{lobCd} AND ITEM_NAME = #{itemName} AND ITEM_DETL = #{itemDetl}")
    int delete2080_01(InFileAu02Vo inFileAu02Vo);
    /////////////////////////////////////////////////////////

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
