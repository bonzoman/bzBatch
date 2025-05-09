package com.bzbatch.sample.config;


import com.bzbatch.common.config.SamgJobExecutionListener;
import com.bzbatch.sample.dto.AutoBatchCommonDto;
import com.bzbatch.sample.dto.InFileAu02Vo;
import com.bzbatch.sample.job.QVUW2080_01Tasklet;
import com.bzbatch.sample.mapper.QVUW2080_01_Query;
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
@ConditionalOnProperty(name = "spring.batch.job.name", havingValue = "QVUWDC_20800")
@Slf4j
public class QVUW2080JobConfig {
    @Bean
    public Job qvuw2080Job(JobRepository jobRepository, Step qvuw2080step, SamgJobExecutionListener samgListener) {
        return new JobBuilder("QVUWDC_20800", jobRepository)
                .start(qvuw2080step)
                .incrementer(new RunIdIncrementer())  // 중요!
                .listener(samgListener)
                .build();
    }

    @Bean
    public Step qvuw2080step(JobRepository jobRepository,
                             PlatformTransactionManager transactionManager,
                             Tasklet tasklet,
                             FlatFileItemReader<InFileAu02Vo> fileReader,
                             FlatFileItemWriter<AutoBatchCommonDto> fileWriter,
                             FlatFileItemWriter<AutoBatchCommonDto> errfileWriter) {
        //???????
        return new StepBuilder("qvuw2080step", jobRepository)
                .tasklet(tasklet, transactionManager)
                .stream(fileReader)
                .stream(fileWriter)
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
    public Tasklet tasklet(QVUW2080_01_Query query,
                           FlatFileItemReader<InFileAu02Vo> fileReader,
                           FlatFileItemWriter<AutoBatchCommonDto> fileWriter,
                           FlatFileItemWriter<AutoBatchCommonDto> errfileWriter,
                           PlatformTransactionManager transactionManager //Note: 트랜잭션 제어를 위해 추가
    ) {
        return QVUW2080_01Tasklet.builder()
                .qvuw208001Query(query)
                .fileReader(fileReader)
                .fileWriter(fileWriter)
                .errfileWriter(errfileWriter)
                .transactionManager(transactionManager)              //Note: 트랜잭션 제어를 위해 추가
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<InFileAu02Vo> fileReader(@Value("#{jobParameters['ODATE']}") String date,
                                                       @Value("#{jobParameters['TIME']}") String time) {
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
    public FlatFileItemWriter<AutoBatchCommonDto> fileWriter(@Value("#{jobParameters['ODATE']}") String date,
                                                             @Value("#{jobParameters['TIME']}") String time) {
        return new FlatFileItemWriterBuilder<AutoBatchCommonDto>()
                .name("fileWriter")
                .resource(new FileSystemResource("/batchlog/ZU2080." + date + "." + time + ".LOG.OUT"))
                .encoding("EUC-KR")
                .delimited().delimiter("^")
                .names("commonString")
                //↓ 여기서 추가 가능
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
                .resource(new FileSystemResource("/batchlog/ZU2080." + date + "." + time + ".ERR.OUT"))
                .encoding("EUC-KR")
                .delimited().delimiter("^")
                .names("commonString")
                .build();
    }
}