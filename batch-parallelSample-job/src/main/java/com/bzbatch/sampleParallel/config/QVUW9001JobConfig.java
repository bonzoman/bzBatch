package com.bzbatch.sampleParallel.config;


import com.bzbatch.common.listener.SamgJobExecutionListener;
import com.bzbatch.common.util.FileMerger;
import com.bzbatch.sampleParallel.dto.InFileAu02Vo;
import com.bzbatch.sampleParallel.dto.ReadDto;
import com.bzbatch.sampleParallel.mapper.QVUW_Query;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisPagingItemReader;
import org.mybatis.spring.batch.builder.MyBatisPagingItemReaderBuilder;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;


@Configuration
@ConditionalOnProperty(name = "spring.batch.job.name", havingValue = "QVUW9001")
@Slf4j
public class QVUW9001JobConfig {

    private static final int PARALLEL_SIZE = 10;
    private static final String OUTPUT_DIR = "D:/batchlog";
    private static final String OUTPUT_PREFIX = "output_";
    private static final String OUTPUT_EXT = ".txt";
    private static final String MERGED_FILE = "output.txt";

    @Bean(name = "parallelTaskExecutor")
    public TaskExecutor parallelTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);// 항상 유지되는 스레드 수
        executor.setMaxPoolSize(20); // 최대 스레드 수
        //executor.setQueueCapacity(10); // 대기 큐에 넣을 수 있는 작업 수
        executor.setThreadNamePrefix("parallel-file-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
//        executor.setTaskDecorator();
        executor.setDaemon(true);
        executor.initialize();
        return executor;
    }

    @Bean
    public Job qvuw9001Job(JobRepository jobRepository,
                           Step mainPartitionStep,
                           Step mergeStep,
                           SamgJobExecutionListener jobExecutionListener) {
        return new JobBuilder("QVUW9001", jobRepository)
                .start(mainPartitionStep)
                .next(mergeStep)
                .listener(jobExecutionListener)
                .build();
    }

    @Bean
    public Step mainPartitionStep(JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager,
                                  MyBatisPagingItemReader<InFileAu02Vo> reader,
                                  FlatFileItemWriter<InFileAu02Vo> writer,
                                  QVUW_Query mapper,
                                  @Qualifier("parallelTaskExecutor") TaskExecutor parallelTaskExecutor) {

        Step subParallelStep = new StepBuilder("subParallelStep", jobRepository)
                .<InFileAu02Vo, InFileAu02Vo>chunk(1000, transactionManager)
                .reader(reader)
                .writer(writer)
                .allowStartIfComplete(true)
                .build();

        return new StepBuilder("mainPartitionStep", jobRepository)
                .partitioner(subParallelStep.getName(), new Partitioner() {
                    @NonNull
                    @Override
                    public Map<String, ExecutionContext> partition(int gridSize) {
                        List<ReadDto> readDtoList = mapper.selectStartEndId(PARALLEL_SIZE);
                        Map<String, ExecutionContext> result = new HashMap<>();
                        for (int i = 0; i < readDtoList.size(); i++) {
                            ReadDto dto = readDtoList.get(i);
                            ExecutionContext extCtx = new ExecutionContext();
                            extCtx.putString("startId", dto.getStartId());
                            extCtx.putString("endId", dto.getEndId());
                            extCtx.putString("partitionIndex", String.format("%02d", i + 1));
                            result.put("partition" + i, extCtx);
                        }
                        return result;
                    }
                })
                .step(subParallelStep)
                .gridSize(PARALLEL_SIZE)
                .taskExecutor(parallelTaskExecutor)
                .listener(new StepExecutionListener() {
                    @Override
                    public ExitStatus afterStep(@NonNull StepExecution stepExecution) {
                        JobExecution jobExec = stepExecution.getJobExecution();
                        ExecutionContext jobCtx = jobExec.getExecutionContext();

                        List<String> fileNames = jobExec.getStepExecutions().stream()
                                // partition된 subParallelStep 실행들만 골라서
                                .filter(se -> se.getStepName().startsWith("subParallelStep"))
                                .map(se -> {
                                    String idx = se.getExecutionContext().getString("partitionIndex");
                                    return OUTPUT_DIR + "/" + OUTPUT_PREFIX + idx + OUTPUT_EXT;
                                })
                                .collect(Collectors.toList());

                        jobCtx.put("fileNames", fileNames);
                        return stepExecution.getExitStatus();
                    }
                })
                .build();
    }


    @Bean
    @StepScope
    public MyBatisPagingItemReader<InFileAu02Vo> reader(@Value("#{stepExecutionContext['startId']}") String startId,
                                                        @Value("#{stepExecutionContext['endId']}") String endId,
                                                        SqlSessionFactory sqlSessionFactory) {
        return new MyBatisPagingItemReaderBuilder<InFileAu02Vo>()
                .sqlSessionFactory(sqlSessionFactory)
                .queryId("com.bzbatch.sampleParallel.mapper.QVUW_Query.selectRangeData")
                .parameterValues(Map.of("startId", startId, "endId", endId))
                .pageSize(1000)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<InFileAu02Vo> fileWriter(@Value("#{stepExecutionContext['startId']}") String startId,
                                                       @Value("#{stepExecutionContext['endId']}") String endId,
                                                       @Value("#{stepExecutionContext['partitionIndex']}") String partitionIndex
    ) {
        return new FlatFileItemWriterBuilder<InFileAu02Vo>()
                .name("fileWriter")
//                .resource(new FileSystemResource("/batchlog/output_" + startId + "_" + endId + ".txt"))
                .resource(new FileSystemResource("/batchlog/output_" + partitionIndex + ".txt"))
                .delimited()
                .delimiter(",")
                .names("lobCd", "itemName", "itemDetl", "seqNo",
                        "itemDetlAttr01", "itemDetlAttr02", "itemDetlAttr03", "itemDetlAttr04", "itemDetlAttr05")
                .encoding("UTF-8")
                .build();
    }

    /**
     * 모든 파티션 파일을 하나로 병합하고, 병합된 뒤 원본 파일을 삭제하는 Tasklet Step
     */
    @Bean
    public Step mergeStep(JobRepository jobRepository,
                          PlatformTransactionManager transactionManager) {
        return new StepBuilder("mergeStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    StepContext sc = chunkContext.getStepContext();
                    @SuppressWarnings("unchecked")
                    List<String> fileNames = (List<String>) sc.getJobExecutionContext().get("fileNames");

                    List<Path> paths = fileNames.stream()
                            .map(Path::of)
                            .collect(Collectors.toList());

                    if (!paths.isEmpty()) {
                        Path merged = Paths.get(OUTPUT_DIR, MERGED_FILE);
                        FileMerger.mergeFilesInParallel(paths, merged);
                        log.info("Merged {} files into {}", paths.size(), merged);
                        //분할 파일 삭제
                        for (Path p : paths) {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException e) {
                                log.warn("삭제 실패: {}", p, e);
                            }
                        }
                        log.info("Deleted {} original files", paths.size());
                    } else {
                        log.warn("No files passed from mainStep for merging");
                    }

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }
}
