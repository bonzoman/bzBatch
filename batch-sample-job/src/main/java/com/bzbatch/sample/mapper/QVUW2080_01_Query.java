package com.bzbatch.sample.mapper;

import com.bzbatch.sample.dto.InFileAu02Vo;
import lombok.Builder;
import org.apache.ibatis.annotations.Insert;
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

    @Select("""
            SELECT ENAME
              FROM EMP
             WHERE JOB =#{job}
            """)
    String selectManager(String job);

    @Insert("""
            INSERT INTO AU02
                (LOB_CD,ITEM_NAME,ITEM_DETL,SEQ_NO,
                 ITEM_DETL_ATTR01,ITEM_DETL_ATTR02,ITEM_DETL_ATTR03,ITEM_DETL_ATTR04,ITEM_DETL_ATTR05)
            VALUES
                (#{lobCd},#{itemName},#{itemDetl},
                 (SELECT NVL(MAX(SEQ_NO), 0) + 1
                    FROM AU02 WHERE LOB_CD = #{lobCd} AND ITEM_NAME = #{itemName} AND ITEM_DETL = #{itemDetl} ),
                 #{itemAttr01},#{itemAttr02},#{itemAttr03},#{itemAttr04},#{itemAttr05})
            """)
    int insert2080_01(InFileAu02Vo inFileAu02Vo);

    @Insert("DELETE FROM AU02 WHERE LOB_CD = #{lobCd} AND ITEM_NAME = #{itemName} AND ITEM_DETL = #{itemDetl}")
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
