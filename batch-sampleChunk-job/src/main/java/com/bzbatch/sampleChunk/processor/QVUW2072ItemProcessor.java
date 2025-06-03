package com.bzbatch.sampleChunk.processor;

import com.bzbatch.sampleChunk.dto.InFileAu02Vo;
import com.bzbatch.sampleChunk.listener.ChunkTrackingReader;
import com.bzbatch.sampleChunk.mapper.QVUW_Query;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.NonNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class QVUW2072ItemProcessor implements ItemProcessor<InFileAu02Vo, InFileAu02Vo>, StepExecutionListener, ChunkListener {

    private final ChunkTrackingReader<InFileAu02Vo> chunkTrackingReader;
    private final String jobOpt;
    private final Map<String, String> managerCache = new HashMap<>();
    @Autowired
    private QVUW_Query query;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private String manager;
    private int totalItemIndex = 0;
    private int chunkItemIndex = 0;

    // 현재 chunk 대상 아이템들. 외부에서 세팅 필요
    private List<InFileAu02Vo> chunkItems = Collections.emptyList();

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("▣▣▣▣▣▣▣▣▣▣ process.beforeStep");
        ExecutionContext context = stepExecution.getExecutionContext();
        this.manager = context.getString("manager2", "UNKNOWN");
        log.info("[Processor] 매니저 정보 로딩 완료: {}", manager);
    }

    @Override //ChunkListener implements하지 않고, @BeforeChunk 어노테이션으로 사용해도 됨
    public void beforeChunk(ChunkContext chunkContext) {
        log.info("▣▣▣▣▣▣▣▣▣▣ process.beforeChunk");
        chunkItemIndex = 0;
//        chunkItems = (List<InFileAu02Vo>) chunkContext.getStepContext().getStepExecution()
//                .getExecutionContext().get("chunk.items"); // 외부에서 넣어줘야 함

//        Set<String> detlSet = chunkItems.stream()
//                .map(InFileAu02Vo::getItemDetl)
//                .filter(Objects::nonNull)
//                .collect(Collectors.toSet());

//        if (!detlSet.isEmpty()) {
//            List<Map<String, String>> resultList = query.selectManager(detlSet); // IN 조건 사용
//            for (Map<String, String> row : resultList) {
//                managerCache.put(row.get("ITEM_DETL"), row.get("MANAGER"));
//            }
//        }

//        log.debug("캐시된 ITEM_DETL 수: {}", managerCache.size());
    }

    @Override
    public InFileAu02Vo process(@NonNull InFileAu02Vo item) {
        totalItemIndex++;
        chunkItemIndex++;
        for (InFileAu02Vo inFileAu02Vo : chunkTrackingReader.getCurrentChunk()) {
            log.info("읽어지나? {}", inFileAu02Vo);
        }
        log.info("▣▣▣▣▣▣▣▣▣▣ process [{} {}번째 처리] 대상: {}", totalItemIndex, chunkItemIndex, item);

//        String manager = query.selectManager("PRESIDENT");
        String manager = managerCache.getOrDefault(item.getItemDetl(), "DEFAULT");
        log.debug(manager);

//        String manager2 = jdbcTemplate.queryForObject("SELECT ENAME FROM EMP WHERE JOB =?", String.class, "PRESIDENT");
//        log.debug(manager2);


        if ("D".equalsIgnoreCase(jobOpt)) {
        } else if ("S".equalsIgnoreCase(jobOpt)) {
            item.setItemAttr04(manager);
        }
        return item;
    }

    @Override
    public void afterChunk(ChunkContext chunkContext) {

        log.info("▣▣▣▣▣▣▣▣▣▣ process.afterChunk");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("▣▣▣▣▣▣▣▣▣▣ process.afterStep");
        return stepExecution.getExitStatus();
    }
}
