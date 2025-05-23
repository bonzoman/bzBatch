package com.bzbatch.sampleChunk.processor;

import com.bzbatch.sampleChunk.dto.AutoBatchCommonDto;
import com.bzbatch.sampleChunk.dto.InFileAu02Vo;
import com.bzbatch.sampleChunk.mapper.QVUW_Query;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.lang.NonNull;

@Slf4j
@RequiredArgsConstructor
public class QVUW2070ItemProcessor implements ItemProcessor<InFileAu02Vo, AutoBatchCommonDto> {

    private final QVUW_Query query;
    private final String jobOpt;
    private String manager;
    private int count = 0;


    @BeforeStep
    public void retrieveManagerFromContext(StepExecution stepExecution) {
        log.debug("[QVUW2070ItemProcessor]  retrieveManagerFromContext ======");
        ExecutionContext context = stepExecution.getExecutionContext();
        this.manager = context.getString("manager2", "UNKNOWN");
        log.info("[Processor] 매니저 정보 로딩 완료: {}", manager);
    }

    @Override
    public AutoBatchCommonDto process(@NonNull InFileAu02Vo item) {
        count++;
        log.debug("[QVUW2070ItemProcessor]  process ======");
        log.debug("[{}번째 처리] 대상: {}", count, item);

        AutoBatchCommonDto dto = new AutoBatchCommonDto();

        try {
            if ("D".equalsIgnoreCase(jobOpt)) {
                query.delete2080_01(item);
            } else if ("S".equalsIgnoreCase(jobOpt)) {
                query.delete2080_01(item);
                item.setItemAttr04(manager);
                query.insert2080_01(item);
            }

            dto.setCommonString("성공: " + item.getItemName());
        } catch (Exception e) {
            log.warn("[{}번째 처리 실패] {}", count, e.getMessage());
            dto.setCommonString("실패: " + item.getItemName());
        }
        return dto;
    }
}
