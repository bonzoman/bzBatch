package com.bzbatch.sample.job;

import com.bzbatch.sample.dto.AutoBatchCommonDto;
import com.bzbatch.sample.mapper.QVUW2080_01_Query;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.repeat.RepeatStatus;

@RequiredArgsConstructor
@Builder
public class QVUW2080_01Tasklet implements Tasklet {
    private final QVUW2080_01_Query query;
    private final FlatFileItemWriter<AutoBatchCommonDto> fileWriter;
    private final FlatFileItemWriter<AutoBatchCommonDto> errfileWriter;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        JobParameters jobParameters = contribution.getStepExecution().getJobParameters();
        String time = jobParameters.getString("TIME");
        String strAweekAgoDay = "20250101"; // etc logic...
        // logic omitted
        return RepeatStatus.FINISHED;
    }
}