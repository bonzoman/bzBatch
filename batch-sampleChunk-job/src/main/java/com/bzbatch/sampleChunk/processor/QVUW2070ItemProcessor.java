package com.bzbatch.sampleChunk.processor;

import com.bzbatch.sampleChunk.dto.AutoBatchCommonDto;
import com.bzbatch.sampleChunk.dto.InFileAu02Vo;
import com.bzbatch.sampleChunk.mapper.QVUW2070_01_Query;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;

@Slf4j
@RequiredArgsConstructor
public class QVUW2070ItemProcessor implements ItemProcessor<InFileAu02Vo, AutoBatchCommonDto> {

    private final QVUW2070_01_Query query;
    private final String jobOpt;
    //    private final String manager;
    private String manager2;

    @BeforeStep
    public void retrieveManagerFromContext(StepExecution stepExecution) {
        log.debug("[QVUW2070ItemProcessor]  retrieveManagerFromContext ======");
        ExecutionContext context = stepExecution.getExecutionContext();
        this.manager2 = context.getString("manager2", "UNKNOWN");
        log.info("[Processor] 매니저 정보 로딩 완료: {}", manager2);
    }

    @Override
    public AutoBatchCommonDto process(InFileAu02Vo item) throws Exception {
        log.debug("[QVUW2070ItemProcessor]  process ======");

        log.debug("처리 대상: {}", item);

        if ("D".equalsIgnoreCase(jobOpt)) {
            query.delete2080_01(item);
        } else if ("S".equalsIgnoreCase(jobOpt)) {
            query.delete2080_01(item);
//            item.setItemAttr04(manager);
            item.setItemAttr04(manager2);
            query.insert2080_01(item);
        }

        AutoBatchCommonDto dto = new AutoBatchCommonDto();
        dto.setCommonString("성공: " + item.getLobCd());
        return dto;
    }
}
