package com.bzbatch.sampleChunk.config;


import com.bzbatch.common.config.SamgJobExecutionListener;
import com.bzbatch.sampleChunk.dto.AutoBatchCommonDto;
import com.bzbatch.sampleChunk.dto.InFileAu02Vo;
import com.bzbatch.sampleChunk.listener.QVUW2070StepListener;
import com.bzbatch.sampleChunk.mapper.QVUW2070_01_Query;
import com.bzbatch.sampleChunk.processor.QVUW2070ItemProcessor;
import com.bzbatch.sampleChunk.writer.QVUW2070ErrorWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@ConditionalOnProperty(name = "spring.batch.job.name", havingValue = "QVUWDC_20700")
@Slf4j
public class QVUW2070JobConfig {
    @Bean
    public Job qvuw2070Job(JobRepository jobRepository, Step qvuw2070ChunkStep, SamgJobExecutionListener samgJobListener) {
        log.debug("[QVUW2070JobConfig]  qvuw2070Job ======");
        return new JobBuilder("QVUWDC_20700", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(qvuw2070ChunkStep)
                .listener(samgJobListener)
                .build();
    }

    @Bean
    public Step qvuw2070ChunkStep(JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager,
                                  FlatFileItemReader<InFileAu02Vo> fileReader,
                                  QVUW2070ItemProcessor processor,
                                  FlatFileItemWriter<AutoBatchCommonDto> writer,
                                  QVUW2070StepListener qvuw2070StepListener,
                                  QVUW2070ErrorWriter errorWriter
    ) {
        log.debug("[QVUW2070JobConfig]  qvuw2070ChunkStep ======");
        return new StepBuilder("qvuw2070ChunkStep", jobRepository)
                .<InFileAu02Vo, AutoBatchCommonDto>chunk(2, transactionManager)
                .reader(fileReader)
                .processor(processor)
                .writer(writer)
                .faultTolerant() // 예외발생 시 skip/retry 정책
                .skipLimit(10) // 최대 스킵 건수
                .skip(Exception.class) // 스킵할 예외 지정
                .listener(qvuw2070StepListener)
                .listener(errorWriter)
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
                                           QVUW2070_01_Query query) {
        log.debug("[QVUW2070JobConfig]  processor ======");
//        String manager = query.selectManager("PRESIDENT");
//        return new QVUW2070ItemProcessor(query, jobOpt, manager);
        return new QVUW2070ItemProcessor(query, jobOpt);
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<AutoBatchCommonDto> itemWriter(@Value("#{jobParameters['ODATE']}") String date,
                                                             @Value("#{jobParameters['TIME']}") String time) {
        log.debug("[QVUW2070JobConfig]  itemWriter ======");
        return new FlatFileItemWriterBuilder<AutoBatchCommonDto>()
                .name("successWriter")
                .resource(new FileSystemResource("/batchlog/ZU2080." + date + "." + time + ".LOG.OUT"))
                .encoding("EUC-KR")
                .delimited().delimiter("^")
                .names("commonString")
                .build();
    }

    @Bean
    @StepScope
    public QVUW2070ErrorWriter errorWriter(@Value("#{jobParameters['ODATE']}") String date,
                                           @Value("#{jobParameters['TIME']}") String time) {
        log.debug("[QVUW2070JobConfig]  errorWriter ======");
        return new QVUW2070ErrorWriter(date, time);
    }

}