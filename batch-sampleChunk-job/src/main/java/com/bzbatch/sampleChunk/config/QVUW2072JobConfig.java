package com.bzbatch.sampleChunk.config;


import com.bzbatch.common.config.SamgJobExecutionListener;
import com.bzbatch.sampleChunk.dto.AutoBatchCommonDto;
import com.bzbatch.sampleChunk.dto.InFileAu02Vo;
import com.bzbatch.sampleChunk.mapper.QVUW_Query;
import com.bzbatch.sampleChunk.processor.QVUW2072ItemProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisBatchItemWriter;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.classify.Classifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.lang.NonNull;
import org.springframework.transaction.PlatformTransactionManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * <pre>
 * QVUW2070JobConfig
 *
 * 0. 전처리(StepExecutionListener) : 파일체크 및 본처리에서 사용될 값 획득
 * 1. fileReader(inFile read)
 * 2. process(실제 DB처리) 후 성공/실패 log용 dto 리턴
 * 3. write(성공/실패) 로그쓰기
 * </pre>
 */
@Configuration
@ConditionalOnProperty(name = "spring.batch.job.name", havingValue = "QVUWDC_2072")
//@RequiredArgsConstructor
@Slf4j
public class QVUW2072JobConfig {

//    private final QVUW_Query qvuwQuery;

    @Bean
    public Job qvuw2072Job(JobRepository jobRepository, Step qvuw2072ChunkStep, SamgJobExecutionListener samgJobListener) {
        log.info("[QVUW2072JobConfig]  qvuw2072Job ======");
        return new JobBuilder("QVUWDC_2072", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(qvuw2072ChunkStep)
                .listener(samgJobListener)
                .build();
    }

    @Bean
    public Step qvuw2072ChunkStep(JobRepository jobRepository,
                                  @Qualifier("hrTransactionManager")
                                  PlatformTransactionManager transactionManager,
                                  QVUW_Query qvuwQuery,
//                                  QVUW2072StepListener qvuw2072StepListener,
                                  FlatFileItemReader<InFileAu02Vo> fileReader,
                                  QVUW2072ItemProcessor processor,

//                                  MyBatisBatchItemWriter<InFileAu02Vo> myBatisWriter
                                  ItemWriter<InFileAu02Vo> customDbWriterForBatch,
//                                  ItemWriter<InFileAu02Vo> customDbWriterForSimpleManualCommit,
//                                  ItemWriter<InFileAu02Vo> customDbWriterForSimpleAutoCommit,

//                                  ClassifierCompositeItemWriter<AutoBatchCommonDto> compositeWriter,
//                                  @Qualifier("successFileWriter") FlatFileItemWriter<AutoBatchCommonDto> successFileWriter,
                                  FlatFileItemWriter<AutoBatchCommonDto> failFileWriter
//                                  FlatFileItemWriter<AutoBatchCommonDto> writer,
//                                  QVUW2072ErrorWriter errorWriter
    ) {
        log.info("[QVUW2072JobConfig]  qvuw2072ChunkStep ======");
        return new StepBuilder("qvuw2072ChunkStep", jobRepository)
                .<InFileAu02Vo, InFileAu02Vo>chunk(200, transactionManager)
                .reader(fileReader)
                .processor(processor)

//                .writer(myBatisWriter) // DB 작업 처리
                .writer(customDbWriterForBatch) // DB 작업 처리
//                .writer(customDbWriterForSimpleManualCommit) // DB 작업 처리
//                .writer(customDbWriterForSimpleAutoCommit) // DB 작업 처리

//                .writer(compositeWriter)
//                .stream(successFileWriter)  // <- 여기 필수!
//                .stream(failFileWriter)     // <- 여기 필수!
//                .writer(compositeWriter)
//                .writer(writer)
                //NOTE: 이렇게 하면 STEP에서 발생한 모든 오류에 대해 try-catch없이도 skip처리되어 정상처리건은 모두 처리할 수 있게 된다.
                // - 오류로 skip처리된 건은 SkipListener에서 알맞에 처리하면 된다.
                // - 오류시 process가 재호출 되는 문제................
//                .faultTolerant().skipLimit(10000000).skip(Exception.class).listener(errorFileWriterSkipListener(failFileWriter))
//                .listener(qvuw2072StepListener)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(@NonNull StepExecution stepExecution) {
                        log.info("[QVUW2072StepListener]  beforeStep ======");
                        Path input = Paths.get("/batchlog/INFILESAMPLE.IN");
                        Path touch = Paths.get("/batchlog/INFILESAMPLE.IN.TOUCH");

                        if (!Files.exists(input) || !Files.exists(touch)) {
                            //NOTE: STOP종료(오류아님)
                            log.warn("[SKIP] 필수 파일 누락 → Step 강제 중단 처리됨: {}, {}", input, touch);
                            stepExecution.setTerminateOnly();
                            //NOTE: Exception종료(오류)
//                          log.warn("[SKIP] 필수 파일 누락 → Job 정상 종료 처리됨: {}, {}", input, touch);
//                          throw new IllegalStateException("필수 입력 파일이 존재하지 않습니다: " + input + " / " + touch);
                        } else {
                            log.info("✔ 입력 파일 존재 확인 완료: {}, {}", input, touch);
                            // 매니저 이름 조회
                            String manager = qvuwQuery.selectManager("PRESIDENT");

                            // ExecutionContext에 저장
                            stepExecution.getExecutionContext().put("manager", manager);
                            log.info("✔ 매니저 정보 저장 완료: {}", manager);

                        }
                    }

                    @Override
                    public ExitStatus afterStep(@NonNull StepExecution stepExecution) {
                        log.info("[InputFileCheckListener]  afterStep ======");
                        return stepExecution.getExitStatus();
                    }
                })
//                .listener(errorWriter)
                // ↓ 여기서 추가 가능
//                .listener(new StepExecutionListener() { … })                    // Step 전/후 처리 로직
//                .retryLimit(3)                                               // 재시도 횟수
//                .retry(DataAccessException.class)                            // 재시도 대상 예외
//                .listener(fileWriteListener())                                // ItemWriter 전용 리스너
                .build();
    }

//    @Bean
//    public SkipListener<InFileAu02Vo, InFileAu02Vo> errorFileWriterSkipListener(
//            FlatFileItemWriter<AutoBatchCommonDto> failFileWriter
//    ) {
//        return new SkipListener<>() {
//            @Override
//            public void onSkipInProcess(@NonNull InFileAu02Vo item, @NonNull Throwable t) {
//                log.warn("Processor Skip: {}", item, t);
//                writeErrorFile(item, t);
//            }
//
//            @Override
//            public void onSkipInWrite(@NonNull InFileAu02Vo item, @NonNull Throwable t) {
//                log.warn("Writer Skip: {}", item, t);
//                writeErrorFile(item, t);
//            }
//
//            private void writeErrorFile(InFileAu02Vo item, Throwable t) {
//                AutoBatchCommonDto failLog = new AutoBatchCommonDto();
//                failLog.setCommonString("실패: " + item.getLobCd() + "^" + t.getMessage());
//                try {
//                    failFileWriter.write(new Chunk<>(failLog));
//                } catch (Exception ex) {
//                    log.error("ERR 파일 쓰기 실패", ex);
//                }
//            }
//        };
//
//    }


    @Bean
    @StepScope
    public FlatFileItemReader<InFileAu02Vo> fileReader(@Value("#{jobParameters['ODATE']}") String date,
                                                       @Value("#{jobParameters['TIME']}") String time) {
        log.info("[QVUW2072JobConfig]  fileReader ======");
        return new FlatFileItemReaderBuilder<InFileAu02Vo>()
                .name("fileReader")
                .resource(new FileSystemResource("/batchlog/INFILESAMPLE.IN"))
                .encoding("EUC-KR")
                .delimited().delimiter("^")
                .names("lobCd", "itemName", "itemDetl", "itemAttr01", "itemAttr02", "itemAttr03", "itemAttr04", "itemAttr05")
                .targetType(InFileAu02Vo.class)
                .build();
    }

    @Bean
    @StepScope
    public QVUW2072ItemProcessor processor(@Value("#{jobParameters['JOB_OPT']}") String jobOpt,
                                           QVUW_Query query) {
        log.info("[QVUW2072JobConfig]  processor ======");
        return new QVUW2072ItemProcessor(query, jobOpt);
    }

    @Bean
    @StepScope
    public MyBatisBatchItemWriter<InFileAu02Vo> myBatisWriter(SqlSessionFactory sqlSessionFactory,
                                                              @Value("#{jobParameters['JOB_OPT']}") String jobOpt) {
        log.info("[QVUW2072JobConfig]  myBatisWriter ======");

        //NOTE: 쿼리 1개만 실행가능
        return new MyBatisBatchItemWriterBuilder<InFileAu02Vo>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.bzbatch.sampleChunk.mapper.QVUW_Query." +
                        ("D".equalsIgnoreCase(jobOpt) ? "delete2080_01" : "insert2080_01"))
                .assertUpdates(false) // insert/delete 결과 rowCount 체크 여부 (옵션) //NOTE: 처리건수가 1이상이 되면 오류나는걸 방지
                .build();
    }

    //SqlSessionTemplate sqlSessionTemplate,

    /**
     * ExecutorType.BATCH 방식
     *
     * @param sqlSessionFactory
     * @param jobOpt
     * @return
     */
    @Bean
    @StepScope
    public ItemWriter<InFileAu02Vo> customDbWriterForBatch(@Qualifier("manualSqlSessionFactory") SqlSessionFactory sqlSessionFactory,
                                                           @Value("#{jobParameters['JOB_OPT']}") String jobOpt) {
        log.info("customDbWriterForBatch ======");

        return new ItemWriter<InFileAu02Vo>() {
            @Override
            public void write(@NonNull Chunk<? extends InFileAu02Vo> items) {
                SqlSession batchSession = null;
                boolean batchFailed = false;
                try {
                    batchSession = sqlSessionFactory.openSession(ExecutorType.BATCH, false);
                    log.debug("AutoCommit = {}", batchSession.getConnection().getAutoCommit());
                    log.debug("TransactionFactory = {}", sqlSessionFactory.getConfiguration().getEnvironment().getTransactionFactory().getClass());
                    QVUW_Query query = batchSession.getMapper(QVUW_Query.class);
                    for (InFileAu02Vo item : items) {
                        if ("D".equalsIgnoreCase(jobOpt)) {
                            query.delete2080_01(item);
                        } else if ("S".equalsIgnoreCase(jobOpt)) {
//                            query.delete2080_01(item); // 선삭제
                            if (item.getItemDetl().equals("Buto3")) {
                                item.setSeqNo(1);//오류위해 세번째 Dup 오류 발생
                            }
                            query.insert2080_01(item); // 후저장
                        }
                    } // for
                    List<BatchResult> batchResultList = batchSession.flushStatements();
//                    batchResultList.forEach(batchResult -> {
//                        String sql = batchResult.getSql().toLowerCase(); // SQL 문자열 확인
//                        log.debug("{}", batchResult.getUpdateCounts());
//                    });
                    batchSession.commit(); // 커밋 꼭 필요
                } catch (Exception e) {
                    batchFailed = true;
                    log.warn("BATCH 모드 처리 실패. rollback 수행: {}", e.getMessage());
                    if (batchSession != null) {
                        try {
                            batchSession.rollback(); // rollback 가능
                        } catch (Exception rollbackEx) {
                            log.error("rollback 중 오류 발생: {}", rollbackEx.getMessage());
                        }
                    }
                } finally {
                    if (batchSession != null) {
                        batchSession.close(); // 명시적으로 닫기
                    }
                }

                // 2차: BATCH 실패 시, 단건 처리로 재시도
                if (batchFailed) {
                    try (SqlSession session = sqlSessionFactory.openSession(false)) {
                        log.debug("AutoCommit = {}", session.getConnection().getAutoCommit());
                        log.debug("TransactionFactory = {}", sqlSessionFactory.getConfiguration().getEnvironment().getTransactionFactory().getClass());

                        QVUW_Query mapper = session.getMapper(QVUW_Query.class);
                        for (InFileAu02Vo item : items) {
                            try {
                                if ("D".equalsIgnoreCase(jobOpt)) {
                                    mapper.delete2080_01(item);
                                } else if ("S".equalsIgnoreCase(jobOpt)) {
//                            mapper.delete2080_01(item); // 선삭제
                                    if (item.getItemDetl().equals("Buto3")) {
                                        item.setSeqNo(1);//오류위해 세번째 Dup 오류 발생
                                    }
                                    mapper.insert2080_01(item); // 후저장
                                }
                                session.commit(); // 커밋 꼭 필요
                                log.debug("a");
                            } catch (Exception e1) {
                                log.error("Batch DB 처리 실패1", e1);
                                session.rollback();
                            }
                        } // for
                    } catch (Exception e) {
                        log.error("session Exception", e);
                    }
                }

            }
        };
//        //NOTE: 쿼리 N개 실행가능
//        return items -> {
//            try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH, false)) {
//                QVUW_Query mapper = session.getMapper(QVUW_Query.class);
//
//                for (InFileAu02Vo item : items) {
//                    if ("D".equalsIgnoreCase(jobOpt)) {
//                        mapper.delete2080_01(item);
//                    } else if ("S".equalsIgnoreCase(jobOpt)) {
//                        mapper.delete2080_01(item); // 선삭제
//                        mapper.insert2080_01(item); // 후저장
//                    }
//                }
//
//                session.commit(); // 커밋 꼭 필요
//            } catch (Exception e) {
//                log.error("Batch DB 처리 실패", e);
//                throw e; // rollback 유도
//            }
//        };
    }

    /**
     * 처리 건단위 수동 commit
     *
     * @param sqlSessionFactory
     * @param jobOpt
     * @return
     */
    @Bean
    @StepScope
    public ItemWriter<InFileAu02Vo> customDbWriterForSimpleManualCommit(@Qualifier("manualSqlSessionFactory") SqlSessionFactory sqlSessionFactory,
                                                                        @Value("#{jobParameters['JOB_OPT']}") String jobOpt) {
        log.info("[QVUW2072JobConfig]  customDbWriter ======");


        return new ItemWriter<InFileAu02Vo>() {
            private int count = 0;

            @Override
            public void write(@NonNull Chunk<? extends InFileAu02Vo> items) {
                try (SqlSession session = sqlSessionFactory.openSession(false)) {
                    log.debug("AutoCommit = {}", session.getConnection().getAutoCommit());
                    log.debug("TransactionFactory = {}", sqlSessionFactory.getConfiguration().getEnvironment().getTransactionFactory().getClass());

                    QVUW_Query mapper = session.getMapper(QVUW_Query.class);
                    for (InFileAu02Vo item : items) {
                        count++;
                        try {
                            if ("D".equalsIgnoreCase(jobOpt)) {
                                mapper.delete2080_01(item);
                            } else if ("S".equalsIgnoreCase(jobOpt)) {
//                            mapper.delete2080_01(item); // 선삭제
//                                if (item.getItemDetl().equals("Buto3")) {
//                                    item.setSeqNo(1);//오류위해 세번째 Dup 오류 발생
//                                    mapper.insert2080_01(item); // 후저장
//                                }
                                mapper.insert2080_01(item); // 후저장
                            }
                            session.commit(); // 커밋 꼭 필요
                            log.debug("a");
                        } catch (Exception e1) {
                            log.error("Batch DB 처리 실패1", e1);
                            session.rollback();
                        }
                    } // for
                } catch (Exception e) {
                    log.error("session Exception", e);
                }
            }
        };
    }

    /**
     * auto commit
     *
     * @param jobOpt
     * @return
     */
    @Bean
    @StepScope
    public ItemWriter<InFileAu02Vo> customDbWriterForSimpleAutoCommit(QVUW_Query qvuwQuery,
                                                                      @Value("#{jobParameters['JOB_OPT']}") String jobOpt) {
        log.debug("[QVUW2072JobConfig]  customDbWriter ======");

        //NOTE: 쿼리 N개 실행가능
        return items -> {
            for (InFileAu02Vo item : items) {
                try {
                    if ("D".equalsIgnoreCase(jobOpt)) {
                        qvuwQuery.delete2080_01(item);
                    } else if ("S".equalsIgnoreCase(jobOpt)) {
//                        qvuwQuery.delete2080_01(item); // 선삭제

                        if (item.getItemDetl().equals("Buto3")) {
//                            item.setSeqNo(1);//오류위해 세번째 Dup 오류 발생
                            qvuwQuery.insert2080_01(item); // 후저장
                        }
                        qvuwQuery.insert2080_01(item); // 후저장
                    }
                } catch (Exception e) {
                    log.error("Batch DB 처리 실패1", e);
                }
            }
        };
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<AutoBatchCommonDto> successFileWriter(@Value("#{jobParameters['ODATE']}") String date,
                                                                    @Value("#{jobParameters['TIME']}") String time) {
        log.debug("[QVUW2072JobConfig]  successFileWriter ======");
        return new FlatFileItemWriterBuilder<AutoBatchCommonDto>()
                .name("successFileWriter")
                .resource(new FileSystemResource("/batchlog/ZU2080." + date + "." + time + ".LOG.OUT"))
                .encoding("EUC-KR")
                .delimited().delimiter("^")
                .names("commonString")
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<AutoBatchCommonDto> failFileWriter(@Value("#{jobParameters['ODATE']}") String date,
                                                                 @Value("#{jobParameters['TIME']}") String time) {
        log.debug("[QVUW2072JobConfig]  failFileWriter ======");
        return new FlatFileItemWriterBuilder<AutoBatchCommonDto>()
                .name("failFileWriter")
                .resource(new FileSystemResource("/batchlog/ZU2080." + date + "." + time + ".ERR.OUT"))
                .encoding("EUC-KR")
                .delimited().delimiter("^")
                .names("commonString")
                .build();
    }

    @Bean
    @StepScope
    public ClassifierCompositeItemWriter<AutoBatchCommonDto> compositeWriter(
            @Qualifier("successFileWriter") FlatFileItemWriter<AutoBatchCommonDto> successWriter,
            @Qualifier("failFileWriter") FlatFileItemWriter<AutoBatchCommonDto> failWriter) {

        ClassifierCompositeItemWriter<AutoBatchCommonDto> writer = new ClassifierCompositeItemWriter<>();

        writer.setClassifier((Classifier<AutoBatchCommonDto, ItemWriter<? super AutoBatchCommonDto>>) dto -> {
            if (dto.getCommonString() != null && dto.getCommonString().startsWith("실패:")) {
                return failWriter;
            } else {
                return successWriter;
            }
        });

        return writer;
    }

//    @Bean
//    @StepScope
//    public QVUW2072ErrorWriter errorWriter(@Value("#{jobParameters['ODATE']}") String date,
//                                           @Value("#{jobParameters['TIME']}") String time) {
//        log.debug("[QVUW2072JobConfig]  errorWriter ======");
//        return new QVUW2072ErrorWriter(date, time);
//    }

}