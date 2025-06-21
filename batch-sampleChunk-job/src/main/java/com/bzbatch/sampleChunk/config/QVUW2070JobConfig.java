package com.bzbatch.sampleChunk.config;


import com.bzbatch.common.listener.SamgJobExecutionListener;
import com.bzbatch.sampleChunk.dto.AutoBatchCommonDto;
import com.bzbatch.sampleChunk.dto.InFileAu02Vo;
import com.bzbatch.sampleChunk.mapper.QVUW_Query;
import com.bzbatch.sampleChunk.processor.QVUW2070ItemProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
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
@ConditionalOnProperty(name = "spring.batch.job.name", havingValue = "QVUWDC_2070")
@RequiredArgsConstructor
@Slf4j
public class QVUW2070JobConfig {

    private final QVUW_Query qvuwQuery;

    @Bean
    public Job qvuw2070Job(JobRepository jobRepository, Step qvuw2070ChunkStep, SamgJobExecutionListener samgJobListener) {
        log.debug("[QVUW2070JobConfig]  qvuw2070Job ======");
        return new JobBuilder("QVUWDC_2070", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(qvuw2070ChunkStep)
                .listener(samgJobListener)
                .build();
    }

    @Bean
    public Step qvuw2070ChunkStep(JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager,
//                                  QVUW2070StepListener qvuw2070StepListener,
                                  FlatFileItemReader<InFileAu02Vo> fileReader,
                                  QVUW2070ItemProcessor processor,
                                  ClassifierCompositeItemWriter<AutoBatchCommonDto> compositeWriter,
                                  @Qualifier("successFileWriter") FlatFileItemWriter<AutoBatchCommonDto> successFileWriter,
                                  @Qualifier("failFileWriter") FlatFileItemWriter<AutoBatchCommonDto> failFileWriter
//                                  FlatFileItemWriter<AutoBatchCommonDto> writer,
//                                  QVUW2070ErrorWriter errorWriter
    ) {
        log.debug("[QVUW2070JobConfig]  qvuw2070ChunkStep ======");
        return new StepBuilder("qvuw2070ChunkStep", jobRepository)
                .<InFileAu02Vo, AutoBatchCommonDto>chunk(2, transactionManager)
                .reader(fileReader)
                .processor(processor)
                .writer(compositeWriter)
                .stream(successFileWriter)  // <- 여기 필수!
                .stream(failFileWriter)     // <- 여기 필수!
//                .writer(writer)
//                .faultTolerant() // 예외발생 시 skip/retry 정책
//                .skipLimit(10) // 최대 스킵 건수
//                .skip(Exception.class) // 스킵할 예외 지정
//                .listener(qvuw2070StepListener)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(@NonNull StepExecution stepExecution) {
                        log.debug("[QVUW2070StepListener]  beforeStep ======");
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
                        log.debug("[InputFileCheckListener]  afterStep ======");
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


    @Bean
    @StepScope
    public FlatFileItemReader<InFileAu02Vo> fileReader(@Value("#{jobParameters['ODATE']}") String date,
                                                       @Value("#{jobParameters['TIME']}") String time) {
        log.debug("[QVUW2070JobConfig]  fileReader ======");
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
    public QVUW2070ItemProcessor processor(@Value("#{jobParameters['JOB_OPT']}") String jobOpt,
                                           QVUW_Query query) {
        log.debug("[QVUW2070JobConfig]  processor ======");
//        String manager = query.selectManager("PRESIDENT");
//        return new QVUW2070ItemProcessor(query, jobOpt, manager);
        return new QVUW2070ItemProcessor(query, jobOpt);
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<AutoBatchCommonDto> successFileWriter(@Value("#{jobParameters['ODATE']}") String date,
                                                                    @Value("#{jobParameters['TIME']}") String time) {
        log.debug("[QVUW2070JobConfig]  successFileWriter ======");
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
        log.debug("[QVUW2070JobConfig]  failFileWriter ======");
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
//    public QVUW2070ErrorWriter errorWriter(@Value("#{jobParameters['ODATE']}") String date,
//                                           @Value("#{jobParameters['TIME']}") String time) {
//        log.debug("[QVUW2070JobConfig]  errorWriter ======");
//        return new QVUW2070ErrorWriter(date, time);
//    }

}