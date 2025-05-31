package com.bzbatch.sampleChunk.processor;

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
public class QVUW2072ItemProcessor implements ItemProcessor<InFileAu02Vo, InFileAu02Vo> {

    private final QVUW_Query query;
    private final String jobOpt;
    private String manager;
    private int count = 0;


    @BeforeStep
    public void retrieveManagerFromContext(StepExecution stepExecution) {
        log.info("[QVUW2072ItemProcessor]  retrieveManagerFromContext ======");
        ExecutionContext context = stepExecution.getExecutionContext();
        this.manager = context.getString("manager2", "UNKNOWN");
        log.info("[Processor] 매니저 정보 로딩 완료: {}", manager);
    }

    @Override
    public InFileAu02Vo process(@NonNull InFileAu02Vo item) {
        count++;
        log.debug("[QVUW2072ItemProcessor]  process ======");
        log.debug("[{}번째 처리] 대상: {}", count, item);

//        String manager = query.selectManager("PRESIDENT");

//        try {
        if ("D".equalsIgnoreCase(jobOpt)) {
//                query.delete2080_01(item);
        } else if ("S".equalsIgnoreCase(jobOpt)) {
//                query.delete2080_01(item);
            item.setItemAttr04(manager);
//                query.insert2080_01(item);
        }
//        } catch (Exception e) {
//            log.warn("[{}번째 처리 실패] {}", count, e.getMessage());
//        }
        return item;
    }
}
