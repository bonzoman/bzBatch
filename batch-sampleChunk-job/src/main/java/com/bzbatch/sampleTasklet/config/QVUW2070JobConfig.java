package com.bzbatch.sampleTasklet.config;


import com.bzbatch.common.config.SamgJobExecutionListener;
import com.bzbatch.sampleTasklet.dto.AutoBatchCommonDto;
import com.bzbatch.sampleTasklet.dto.InFileAu02Vo;
import com.bzbatch.sampleTasklet.job.QVUW2070_01Tasklet;
import com.bzbatch.sampleTasklet.mapper.QVUW2070_01_Query;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
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
    public Job qvuw2070Job(JobRepository jobRepository, Step qvuw2070step, SamgJobExecutionListener samgListener) {
        log.debug("[JOB] --------------- qvuw2070Job ---------------");
        return new JobBuilder("QVUWDC_20700", jobRepository)
                .start(qvuw2070step)
                .incrementer(new RunIdIncrementer())  // 중요!
                .listener(samgListener)
                .build();
    }

    @Bean
    public Step qvuw2070step(JobRepository jobRepository,
                             PlatformTransactionManager transactionManager,
                             Tasklet tasklet,
                             FlatFileItemReader<InFileAu02Vo> fileReader,
                             FlatFileItemWriter<AutoBatchCommonDto> fileWriter,
                             FlatFileItemWriter<AutoBatchCommonDto> errfileWriter) {
        log.debug("[STEP] --------------- qvuw2070step ---------------");
        return new StepBuilder("qvuw2070step", jobRepository)
                .tasklet(tasklet, transactionManager)
                .stream(fileReader) //NOTE: Step에서 open/close 자동 관리됨 (stream 등록됨) Tasklet에서 open/close 안해도 됨
                .stream(fileWriter) //NOTE: Step에서 open/close 자동 관리됨 (stream 등록됨) Tasklet에서 open/close 안해도 됨
                .stream(errfileWriter)
                .allowStartIfComplete(true)
                // ↓ 여기서 추가 가능
//                .listener(new StepExecutionListener() { … })                    // Step 전/후 처리 로직
//                .faultTolerant()                                               // 예외발생 시 skip/retry 정책
//                .skipLimit(5)                                                // 최대 스킵 건수
//                .skip(SomeBusinessException.class)                           // 스킵할 예외 지정
//                .retryLimit(3)                                               // 재시도 횟수
//                .retry(DataAccessException.class)                            // 재시도 대상 예외
//                .listener(fileWriteListener())                                // ItemWriter 전용 리스너
                .build();
    }

    @Bean
    public Tasklet tasklet(QVUW2070_01_Query query,
                           FlatFileItemReader<InFileAu02Vo> fileReader,
                           FlatFileItemWriter<AutoBatchCommonDto> fileWriter,
                           FlatFileItemWriter<AutoBatchCommonDto> errfileWriter,
                           PlatformTransactionManager transactionManager //Note: 트랜잭션 수동제어를 위해 추가
    ) {
        log.debug("[tasklet] --------------- tasklet ---------------");
        return QVUW2070_01Tasklet.builder()
                .qvuw207001Query(query)
                .fileReader(fileReader)
                .fileWriter(fileWriter)
                .errfileWriter(errfileWriter)
                .transactionManager(transactionManager)              //Note: 트랜잭션 수동제어를 위해 추가
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<InFileAu02Vo> fileReader(@Value("#{jobParameters['ODATE']}") String date,
                                                       @Value("#{jobParameters['TIME']}") String time) {
        log.debug("[fileReader] --------------- fileReader ---------------");


        return new FlatFileItemReaderBuilder<InFileAu02Vo>()
                .name("fileReader")
                .resource(new FileSystemResource("/batchlog/INFILESAMPLE.IN"))
                .encoding("EUC-KR")
                .delimited().delimiter("^")
                .names("lobCd", "itemName", "itemDetl", "itemAttr01", "itemAttr02", "itemAttr03", "itemAttr04", "itemAttr05")
                .targetType(InFileAu02Vo.class)
                .build();
    }

//    @Bean
//    @StepScope
//    public MyBatisBatchItemWriter<InFileAu02Vo> dbWriter() {
//
//        return new MyBatisBatchItemWriterBuilder<InFileAu02Vo>()
//                .
//                .build();
//    }

    @Bean
    @StepScope
    public FlatFileItemWriter<AutoBatchCommonDto> fileWriter(@Value("#{jobParameters['ODATE']}") String date,
                                                             @Value("#{jobParameters['TIME']}") String time) {
        return new FlatFileItemWriterBuilder<AutoBatchCommonDto>()
                .name("fileWriter")
                .resource(new FileSystemResource("/batchlog/ZU2070." + date + "." + time + ".LOG.OUT"))
                .encoding("EUC-KR")
                .delimited().delimiter("^")
                .names("commonString")
                //↓ 여기서 추가 가능 ynk8jma2CVF8dpm.ypu
//                .headerCallback(writer -> writer.write("HEADER1^HEADER2^HEADER3"))   // 헤더 라인
//                .footerCallback(writer -> writer.write("Total:^" + 100))          // 풋터 라인
//                .lineAggregator(new DelimitedLineAggregator<AutoBatchCommonDto>() {  // DTO→String 변환 커스터마이징
//                    {
//                        setDelimiter("^");
//                    }
//
//                    {
//                        setFieldExtractor(new BeanWrapperFieldExtractor<>() {{
//                            setNames(new String[]{"field1", "field2"});
//                        }});
//                    }
//                })
//                .shouldDeleteIfExists(true)                                          // 이미 파일 있으면 덮어쓰기
//                .append(true)                                                 // 이어쓰기 모드
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<AutoBatchCommonDto> errfileWriter(@Value("#{jobParameters['ODATE']}") String date,
                                                                @Value("#{jobParameters['TIME']}") String time) {
        //???????
        return new FlatFileItemWriterBuilder<AutoBatchCommonDto>()
                .name("errfileWriter")
                .resource(new FileSystemResource("/batchlog/ZU2070." + date + "." + time + ".ERR.OUT"))
                .encoding("EUC-KR")
                .delimited().delimiter("^")
                .names("commonString")
                .build();
    }
}