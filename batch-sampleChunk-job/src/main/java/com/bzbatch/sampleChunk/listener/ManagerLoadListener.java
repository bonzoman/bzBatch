package com.bzbatch.sampleChunk.listener;

import com.bzbatch.sampleChunk.mapper.QVUW2070_01_Query;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ManagerLoadListener implements StepExecutionListener {

    private final QVUW2070_01_Query query;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.debug("[ManagerLoadListener]  beforeStep ======");
        // 매니저 이름 조회
        String manager = query.selectManager("PRESIDENT");

        // ExecutionContext에 저장
        stepExecution.getExecutionContext().put("manager", manager);
        log.info("✔ 매니저 정보 저장 완료: {}", manager);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.debug("[ManagerLoadListener]  afterStep ======");
        return stepExecution.getExitStatus();
    }
}