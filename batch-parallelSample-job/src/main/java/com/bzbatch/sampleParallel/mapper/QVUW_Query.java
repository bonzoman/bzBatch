package com.bzbatch.sampleParallel.mapper;

import com.bzbatch.sampleParallel.dto.InFileAu02Vo;
import com.bzbatch.sampleParallel.dto.ReadDto;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface QVUW_Query {

    @Select("""
            SELECT
            	  MIN(seqNo)   AS startId
            	, MAX(seqNo)   AS endId
            	, COUNT(seqNo) AS perCntId
            	, MAX(totCnt)  AS totCntId
            FROM (
            	SELECT
            		  SEQ_NO AS seqNo
            		, NTILE(#{divideRows}) OVER(ORDER BY SEQ_NO) AS tile
            		, COUNT(*) OVER() AS totCnt
            	FROM AU02
            ) A
            GROUP BY tile
            """)
    List<ReadDto> selectStartEndId(int divideRows);

    @Select("""
            SELECT * FROM (
                SELECT ROWNUM AS rn, inner_query.*
                FROM (
                    SELECT
                      LOB_CD    as lobCd
                    , ITEM_NAME as itemName
                    , ITEM_DETL as itemDetl
                    , SEQ_NO    AS seqNo
                    , ITEM_DETL_ATTR01 AS itemDetlAttr01
                    , ITEM_DETL_ATTR02 AS itemDetlAttr02
                    , ITEM_DETL_ATTR03 AS itemDetlAttr03
                    , ITEM_DETL_ATTR04 AS itemDetlAttr04
                    , ITEM_DETL_ATTR05 AS itemDetlAttr05
                    FROM AU02
                    WHERE 1=1
                    AND SEQ_NO BETWEEN #{startId} AND #{endId}
                    ORDER BY SEQ_NO
                ) inner_query
                WHERE ROWNUM <= #{_skiprows} + #{_pagesize}
            )
            WHERE rn > #{_skiprows}
            """)
    List<InFileAu02Vo> selectRangeData(int startId, int endId);

}
