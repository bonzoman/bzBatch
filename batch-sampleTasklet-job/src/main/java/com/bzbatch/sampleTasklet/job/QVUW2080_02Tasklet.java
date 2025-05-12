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
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@Builder
public class QVUW2080_02Tasklet implements Tasklet {
    private final QVUW2080_01_Query qvuw208001Query;
    private final FlatFileItemReader<InFileAu02Vo> fileReader;
    private final FlatFileItemWriter<AutoBatchCommonDto> fileWriter;
    private final FlatFileItemWriter<AutoBatchCommonDto> errfileWriter;
    private final SqlSessionFactory sqlSessionFactory;//Note: 트랜잭션 수동제어를 위해 추가
    private final int BATCH_SIZE = 10;
    private int gCount = 5;

    @Override
    public RepeatStatus execute(@NonNull StepContribution contribution, @NonNull ChunkContext chunkContext) throws Exception {
        JobParameters jobParameters = contribution.getStepExecution().getJobParameters();
        String time = jobParameters.getString("TIME");
        String JOB_OPT = jobParameters.getString("JOB_OPT");

        int batchCount = 0;
        int readCount = 0;
        int succCount = 0;
        int failCount = 0;
        try {
//            String StepExecution_Context_PUT_1 = chunkContext.getStepContext()
//                    .getStepExecution().getExecutionContext().getString("StepExecution_Context_PUT_1", "UNKNOWN");

//            log.debug(StepExecution_Context_PUT_1);
            log.debug("{}", gCount++);
            DefaultTransactionDefinition def = new DefaultTransactionDefinition();//Note: 트랜잭션 수동제어를 위해 추가
            def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);//Note: 트랜잭션 수동제어를 위해 추가


            String strAweekAgoDay = "20250101"; // etc logic...


            Path inFilePath = Paths.get("/batchlog/INFILESAMPLE.IN");
            Path touchFilePath = Paths.get("/batchlog/INFILESAMPLE.IN.TOUCH");

            if (!Files.exists(touchFilePath) || !Files.exists(inFilePath)) {//두 파일 모두 존재해야 함
                log.debug("파일 없음: {}", touchFilePath.getFileName());
                log.warn("[SKIP] 필수 파일 누락 → Job 정상 종료 처리됨: {}, {}", touchFilePath, inFilePath);
                return RepeatStatus.FINISHED;
            }
            String manager = qvuw208001Query.selectManager("PRESIDENT");


//        fileReader.open(new ExecutionContext()); // 수동 open

            InFileAu02Vo inFileAu02Vo;

            SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH, false);
            try {
                if ("D".equalsIgnoreCase(JOB_OPT)) { //삭제모드
                    while ((inFileAu02Vo = fileReader.read()) != null) {
                        try {
                            readCount++;
                            log.debug("Read inFileAu02Vo: " + inFileAu02Vo.getLobCd() + ", " + inFileAu02Vo.getItemDetl());

                            //기존 건 delete
                            int deleteCount = qvuw208001Query.delete2080_01(inFileAu02Vo);

                            batchCount++;
                            succCount++;

                            if (batchCount % BATCH_SIZE == 0) {
                                session.flushStatements();
                                session.commit();
                                session.clearCache();
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            failCount++;
                        }
                    }
                } else if ("S".equalsIgnoreCase(JOB_OPT)) { //저장모드
                    while ((inFileAu02Vo = fileReader.read()) != null) {
                        try {
                            readCount++;
                            log.debug("Read inFileAu02Vo: " + inFileAu02Vo.getLobCd() + ", " + inFileAu02Vo.getItemDetl());

                            //기존 건 delete
                            int deleteCount = qvuw208001Query.delete2080_01(inFileAu02Vo);

                            //신규 insert
                            inFileAu02Vo.setItemAttr04(manager);
                            int insertCount = qvuw208001Query.insert2080_01(inFileAu02Vo);
                            batchCount++;
                            succCount++;

                            if (batchCount % BATCH_SIZE == 0) {
                                session.flushStatements();
                                session.commit();
                                session.clearCache();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            failCount++;
                        }
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
                session.close();
                log.info("전체 처리결과: 총건수={}, 성공={}, 실패={}", readCount, succCount, failCount);
            }

//        fileReader.close();//수동 close


//            List<QVUW2080_01_Query.SamgSrchResVo> samgSrchResVoList = qvuw208001Query.select2080_01("MV");
//            for (QVUW2080_01_Query.SamgSrchResVo vo : samgSrchResVoList) {
//            }

            AutoBatchCommonDto succLogDto = new AutoBatchCommonDto();
            succLogDto.setCommonString("success" + gCount);

            fileWriter.write(new Chunk<>(succLogDto));

            AutoBatchCommonDto failLogDto = new AutoBatchCommonDto();
            failLogDto.setCommonString("fail" + gCount);
            errfileWriter.write(new Chunk<>(failLogDto));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            log.info("〓〓〓〓〓〓〓〓〓〓〓〓 JOB_OPT : {}", JOB_OPT);
            log.info("〓〓〓〓〓〓〓〓〓〓〓〓 readCount : {}", readCount);
            log.info("〓〓〓〓〓〓〓〓〓〓〓〓 succCount : {}", succCount);
            log.info("〓〓〓〓〓〓〓〓〓〓〓〓 failCount : {}", failCount);
//            transactionManager.commit(); //todo
        }

//        return RepeatStatus.CONTINUABLE;
        return RepeatStatus.FINISHED;
    }
}