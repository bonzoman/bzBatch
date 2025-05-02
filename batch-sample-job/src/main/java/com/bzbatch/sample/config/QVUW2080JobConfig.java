package com.bzbatch.sample.config;

import com.bzbatch.sample.dto.AutoBatchCommonDto;
import com.bzbatch.sample.job.QVUW2080_01Tasklet;
import com.bzbatch.sample.mapper.QVUW2080_01_Query;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@ConditionalOnProperty(name = "spring.batch.job.name", havingValue = "QVUWDC_20800")
public class QVUW2080JobConfig {
    @Bean
    public Job qvuw2080Job(JobRepository jobRepository, Step qvuw2080step, JobExecutionListener listener) {
        return new JobBuilder("QVUWDC_20800", jobRepository)
                .start(qvuw2080step)
                .listener(listener)
                .build();
    }

    @Bean
    public Step qvuw2080step(JobRepository jobRepository,
                             PlatformTransactionManager transactionManager,
                             Tasklet tasklet,
                             FlatFileItemWriter<AutoBatchCommonDto> fileWriter,
                             FlatFileItemWriter<AutoBatchCommonDto> errfileWriter) {
        return new StepBuilder("qvuw2080step", jobRepository)
                .tasklet(tasklet, transactionManager)
                .stream(fileWriter)
                .stream(errfileWriter)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Tasklet tasklet(QVUW2080_01_Query query,
                           FlatFileItemWriter<AutoBatchCommonDto> fileWriter,
                           FlatFileItemWriter<AutoBatchCommonDto> errfileWriter) {
        return QVUW2080_01Tasklet.builder()
                .query(query)
                .fileWriter(fileWriter)
                .errfileWriter(errfileWriter)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<AutoBatchCommonDto> fileWriter(@Value("#{jobParameters['ODATE']}") String date,
                                                             @Value("#{jobParameters['TIME']}") String time) {
        return new FlatFileItemWriterBuilder<AutoBatchCommonDto>()
                .name("fileWriter")
                .resource(new FileSystemResource("/PQ/QVU/ZU2080." + date + "." + time + ".LOG.OUT"))
                .encoding("EUC-KR")
                .delimited().delimiter("^")
                .names("CommString")
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<AutoBatchCommonDto> errfileWriter(@Value("#{jobParameters['ODATE']}") String date,
                                                                @Value("#{jobParameters['TIME']}") String time) {
        return new FlatFileItemWriterBuilder<AutoBatchCommonDto>()
                .name("errfileWriter")
                .resource(new FileSystemResource("/PQ/QVU/ZU2080." + date + "." + time + ".ERR.OUT"))
                .encoding("EUC-KR")
                .delimited().delimiter("^")
                .names("CommString")
                .build();
    }
}