package com.bzbatch.sample.job;

import com.bzbatch.sample.dto.AutoBatchCommonDto;
import com.bzbatch.sample.dto.InFileAu02Vo;
import com.bzbatch.sample.mapper.QVUW2080_01_Query;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.util.List;

@Slf4j
@Builder
public class QVUW2080_01Tasklet implements Tasklet {
    private final QVUW2080_01_Query qvuw208001Query;
    private final FlatFileItemReader<InFileAu02Vo> fileReader;
    private final FlatFileItemWriter<AutoBatchCommonDto> fileWriter;
    private final FlatFileItemWriter<AutoBatchCommonDto> errfileWriter;
    private final PlatformTransactionManager transactionManager;
    private int gCount = 5;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.debug("{}", gCount++);
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);


        JobParameters jobParameters = contribution.getStepExecution().getJobParameters();
        String time = jobParameters.getString("TIME");
        String strAweekAgoDay = "20250101"; // etc logic...

//        fileReader.open(new ExecutionContext()); // 수동 open

        InFileAu02Vo inFileAu02Vo;
        while ((inFileAu02Vo = fileReader.read()) != null) {
            log.debug("Read inFileAu02Vo: " + inFileAu02Vo.getLobCd() + ", " + inFileAu02Vo.getItemDetl());
        }
//        fileReader.close();//수동 close


        List<QVUW2080_01_Query.SamgSrchResVo> samgSrchResVoList = qvuw208001Query.select2080_01("MV");
        for (QVUW2080_01_Query.SamgSrchResVo vo : samgSrchResVoList) {
            TransactionStatus status = transactionManager.getTransaction(def);
            try {

                transactionManager.commit(status);
            } catch (Exception e) {
                transactionManager.rollback(status);
            }


        }

        AutoBatchCommonDto succLogDto = new AutoBatchCommonDto();
        succLogDto.setCommonString("success" + gCount);

        fileWriter.write(new Chunk<>(succLogDto));

        AutoBatchCommonDto failLogDto = new AutoBatchCommonDto();
        failLogDto.setCommonString("fail" + gCount);
        errfileWriter.write(new Chunk<>(failLogDto));

//        return RepeatStatus.CONTINUABLE;
        return RepeatStatus.FINISHED;
    }
}