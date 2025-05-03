package com.bzbatch.common.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SamgJobExecutionListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("[SAMG] >>>> 배치 Job 시작: {}", jobExecution.getJobInstance().getJobName());
        log.info("[SAMG] >>>> Job 파라미터: {}", jobExecution.getJobParameters());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("[SAMG] >>>> 배치 Job 종료: {}", jobExecution.getJobInstance().getJobName());
        log.info("[SAMG] >>>> 최종 상태: {}", jobExecution.getStatus());

        if (!jobExecution.getAllFailureExceptions().isEmpty()) {
            log.error("[SAMG] >>>> 예외 발생 내역:");
            jobExecution.getAllFailureExceptions()
                    .forEach(ex -> log.error(" - {}", ex.getMessage(), ex));
        }
    }
}
