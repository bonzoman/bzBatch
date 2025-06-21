package com.bzbatch.sampleTasklet.job;

import com.bzbatch.sampleTasklet.dto.AutoBatchCommonDto;
import com.bzbatch.sampleTasklet.dto.InFileAu02Vo;
import com.bzbatch.sampleTasklet.mapper.QVUW2080_01_Query;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.lang.NonNull;

@Slf4j
@Builder
public class QVUW2080_03TaskletSimple implements Tasklet {
    private final FlatFileItemReader<InFileAu02Vo> fileReader;
    private final FlatFileItemWriter<AutoBatchCommonDto> fileWriter;
    private final FlatFileItemWriter<AutoBatchCommonDto> errfileWriter;
    private final SqlSessionFactory sqlSessionFactory;
    private final int BATCH_SIZE = 100;

    @Override
    public RepeatStatus execute(@NonNull StepContribution contribution, @NonNull ChunkContext chunkContext) {
        int batchCount = 0;
        int readCount = 0;
        int succCount = 0;
        int failCount = 0;
        SqlSession session = null;
        try {
            session = sqlSessionFactory.openSession(ExecutorType.BATCH, false);
            QVUW2080_01_Query qvuw208001Query = session.getMapper(QVUW2080_01_Query.class);

            JobParameters jobParameters = contribution.getStepExecution().getJobParameters();
            String JOB_OPT = jobParameters.getString("JOB_OPT");


            String manager = qvuw208001Query.selectManager("PRESIDENT");
            InFileAu02Vo inFileAu02Vo;

            int i = 0;
            while ((inFileAu02Vo = fileReader.read()) != null) {
                try {
                    AutoBatchCommonDto succLogDto = new AutoBatchCommonDto();
                    readCount++;
                    //기존 건 delete
                    //int deleteCount = qvuw208001Query.delete2080_01(inFileAu02Vo);

                    //신규 insert
                    inFileAu02Vo.setItemAttr04(manager);
                    inFileAu02Vo.setSeqNo(++i);
                    int insertCount = qvuw208001Query.insert2080_01(inFileAu02Vo);
                    batchCount++;
                    succCount++;
                    succLogDto.setCommonString("success");
                    fileWriter.write(new Chunk<>(succLogDto));

                    if (batchCount % BATCH_SIZE == 0) {
                        session.flushStatements();
                        session.commit();
                        session.clearCache();
                    }
                } catch (Exception e) {
                    session.rollback();
                    failCount++;
                    AutoBatchCommonDto failLogDto = new AutoBatchCommonDto();
                    failLogDto.setCommonString("fail");
                    errfileWriter.write(new Chunk<>(failLogDto));
                }
            }

            // 남은 배치 처리
            if (batchCount % BATCH_SIZE != 0) {
                session.flushStatements();
                session.commit();
            }
        } catch (Exception e) {
            log.error("전체 처리 중 예외", e);
        } finally {
            if (session != null) session.close();
            log.info("전체 처리결과: 총건수={}, 성공={}, 실패={}", readCount, succCount, failCount);
        }

        return RepeatStatus.FINISHED;
    }
}