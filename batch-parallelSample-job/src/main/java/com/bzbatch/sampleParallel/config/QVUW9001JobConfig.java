package com.bzbatch.sampleParallel.config;


import com.bzbatch.common.listener.SamgJobExecutionListener;
import com.bzbatch.common.util.FileMerger;
import com.bzbatch.sampleParallel.dto.InFileAu02Vo;
import com.bzbatch.sampleParallel.dto.ReadDto;
import com.bzbatch.sampleParallel.mapper.QVUW_Query;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisBatchItemWriter;
import org.mybatis.spring.batch.MyBatisPagingItemReader;
import org.mybatis.spring.batch.builder.MyBatisBatchItemWriterBuilder;
import org.mybatis.spring.batch.builder.MyBatisPagingItemReaderBuilder;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
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
    private static final int CHUNK_SIZE = 1000000;
    private static final String WORK_DIR = "D:/batchlog";
    private static final String OUTPUT_PREFIX = "backup_";
    private static final String OUTPUT_EXT = ".txt";
    private static final String MERGED_FILE = "backup.txt";
    private static final String INPUT_FILE = "input.txt";
    private static final String SPLIT_PREFIX = "input_";
    private static final String SPLIT_EXT = ".txt";

    @Bean(name = "parallelTaskExecutor")
    public TaskExecutor parallelTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(PARALLEL_SIZE);// 항상 유지되는 스레드 수
        executor.setMaxPoolSize(PARALLEL_SIZE * 2); // 최대 스레드 수
        executor.setQueueCapacity(0); // core가 다 차면 바로 스레드 생성하도록 큐 용량 0
        executor.setThreadNamePrefix("parallel-file-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
//        executor.setTaskDecorator();
        executor.setDaemon(true);
        executor.initialize();
        return executor;
    }

    @Bean
    public Job qvuw9001Job(JobRepository jobRepository,
                           Step dummyStartStep,
                           Step mainPartitionStep,
                           Step mergeStep,
                           Step truncateStep,
                           Step fileSplitStep,
                           Step insertMainStep,
                           PlatformTransactionManager transactionManager,
                           SamgJobExecutionListener jobExecutionListener) {
        return new JobBuilder("QVUW9001", jobRepository)
                .start(dummyStartStep)
                .start(mainPartitionStep)
                .next(mergeStep)
//                .next(truncateStep)
//                .next(fileSplitStep)
//                .next(insertMainStep)
                .listener(jobExecutionListener)
                .build();
    }

    /**
     * 아무 일도 하지 않고 바로 종료되는 더미 스텝
     */
    @Bean
    public Step dummyStartStep(JobRepository jobRepository,
                               PlatformTransactionManager transactionManager) {
        return new StepBuilder("dummyStartStep", jobRepository)
                .tasklet((contribution, chunkContext) -> RepeatStatus.FINISHED, transactionManager)
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
                .<InFileAu02Vo, InFileAu02Vo>chunk(CHUNK_SIZE, transactionManager)
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
                                    return WORK_DIR + "/" + OUTPUT_PREFIX + idx + OUTPUT_EXT;
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
                .pageSize(CHUNK_SIZE)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<InFileAu02Vo> fileWriter(@Value("#{stepExecutionContext['startId']}") String startId,
                                                       @Value("#{stepExecutionContext['endId']}") String endId,
                                                       @Value("#{stepExecutionContext['partitionIndex']}") String partitionIndex
    ) {

//        FlatFileItemWriter<InFileAu02Vo> writer = new FlatFileItemWriter<>();
//        writer.setName("fileWriter");
//        writer.setEncoding("UTF-8");
//        writer.setResource(new FileSystemResource(WORK_DIR + "/" + OUTPUT_PREFIX + partitionIndex + ".txt"));
//
//        // CSV 설정
//        DelimitedLineAggregator<InFileAu02Vo> aggregator = new DelimitedLineAggregator<>();
//        aggregator.setDelimiter(",");
//        BeanWrapperFieldExtractor<InFileAu02Vo> extractor = new BeanWrapperFieldExtractor<>();
//        extractor.setNames(new String[]{"lobCd", "itemName", "itemDetl", "seqNo",
//                "itemDetlAttr01", "itemDetlAttr02", "itemDetlAttr03", "itemDetlAttr04", "itemDetlAttr05"});
//        aggregator.setFieldExtractor(extractor);
//        writer.setLineAggregator(aggregator);
//
//        // 중요: flush 횟수 줄이기
//        writer.setTransactional(false); // 트랜잭션 경계 없이 파일 버퍼 유지 → flush 줄어듦
//
//        return writer;

        return new FlatFileItemWriterBuilder<InFileAu02Vo>()
                .name("fileWriter")
                .resource(new FileSystemResource(WORK_DIR + "/" + OUTPUT_PREFIX + partitionIndex + ".txt"))
                .delimited()
                .delimiter(",")
                .names("lobCd", "itemName", "itemDetl", "seqNo",
                        "itemDetlAttr01", "itemDetlAttr02", "itemDetlAttr03", "itemDetlAttr04", "itemDetlAttr05")
                .encoding("UTF-8")
                .transactional(false)
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
                        Path merged = Paths.get(WORK_DIR, MERGED_FILE);
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

    /*
     * 3단계: 병합 후 기존 테이블 TRUNCATE
     */
    @Bean
    public Step truncateStep(JobRepository jobRepository,
                             DataSource dataSource,
                             PlatformTransactionManager transactionManager) {
        return new StepBuilder("truncateStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    JdbcTemplate jdbc = new JdbcTemplate(dataSource);
                    jdbc.execute("TRUNCATE TABLE AU02");
                    log.info("Truncated table AU02");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    /**
     * 4단계. infile.txt를 10개의 infile_XX.txt 로 나눠 쓰는 Tasklet
     */
    @Bean
    public Step fileSplitStep(JobRepository jobRepository,
                              PlatformTransactionManager transactionManager) {
        return new StepBuilder("fileSplitStep", jobRepository)
                .tasklet((contrib, ctx) -> {
                    List<String> lines = Files.readAllLines(Paths.get(WORK_DIR + "/" + INPUT_FILE));
                    int total = lines.size();
                    int chunk = (total + PARALLEL_SIZE - 1) / PARALLEL_SIZE;

                    for (int i = 0; i < PARALLEL_SIZE; i++) {
                        int start = i * chunk;
                        int end = Math.min(start + chunk, total);
                        if (start >= end) break;
                        Path part = Paths.get(WORK_DIR,
                                SPLIT_PREFIX + String.format("%02d", i + 1) + SPLIT_EXT);
                        Files.write(part, lines.subList(start, end));
                    }
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    /**
     * 5단계. infile_*.txt 를 MultiResourcePartitioner 로 분할 → subStep 에서 읽고 DB에 APPEND INSERT
     */
    @Bean
    public Step insertMainStep(JobRepository jobRepository,
                               PlatformTransactionManager txManager,
                               FlatFileItemReader<InFileAu02Vo> fileReader,
                               MyBatisBatchItemWriter<InFileAu02Vo> dbWriter,
                               SqlSessionFactory sqlSessionFactory,
                               TaskExecutor parallelTaskExecutor) throws IOException {
        // 1) 파티셔너 생성
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("file:" + WORK_DIR + "/" + SPLIT_PREFIX + "*.txt");
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        partitioner.setKeyName("file");
        partitioner.setResources(resources);

        // 2) 파티션마다 실행할 SubStep을 메서드 내에서 정의
        Step importSubStep = new StepBuilder("importSubStep", jobRepository)
                .<InFileAu02Vo, InFileAu02Vo>chunk(1000, txManager)
//                .reader(fileReader(null)) // resource는 런타임에 주입됨 // <-- null 넣어도, 실제로는 StepScope에서 채워짐
//                .writer(dbWriter(sqlSessionFactory))
                .reader(fileReader)
                .writer(dbWriter)
                .build();

        return new StepBuilder("insertMainStep", jobRepository)
                .partitioner("importSubStep", partitioner)
                .step(importSubStep)
                .gridSize(PARALLEL_SIZE)
                .taskExecutor(parallelTaskExecutor)
                .build();
    }


    /**
     * 각 분할 파일을 읽는 FlatFileItemReader
     */
    @Bean
    @StepScope
    public FlatFileItemReader<InFileAu02Vo> fileReader(@Value("#{stepExecutionContext['file']}") Resource resource) {
        return new FlatFileItemReaderBuilder<InFileAu02Vo>()
                .name("fileReader")
                .resource(resource)
                .delimited()
                .delimiter(",")
                .names("lobCd", "itemName", "itemDetl", "seqNo",
                        "itemDetlAttr01", "itemDetlAttr02", "itemDetlAttr03",
                        "itemDetlAttr04", "itemDetlAttr05")
                .targetType(InFileAu02Vo.class)
                .build();
    }

    /**
     * APPEND 힌트가 포함된 MyBatis INSERT 매퍼 호출
     */
    @Bean
    @StepScope
    public MyBatisBatchItemWriter<InFileAu02Vo> dbWriter(SqlSessionFactory sqlSessionFactory) {
        return new MyBatisBatchItemWriterBuilder<InFileAu02Vo>()
                .sqlSessionFactory(sqlSessionFactory)
                .statementId("com.bzbatch.sampleParallel.mapper.QVUW_Query.insertAu02Append")
                .assertUpdates(false)
                .build();
    }

}
