package com.bzbatch.sampleChunk.listener;

import com.bzbatch.sampleChunk.mapper.QVUW2070_01_Query;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@Component
@RequiredArgsConstructor
public class QVUW2070StepListener implements StepExecutionListener {

    private final QVUW2070_01_Query query;

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
//            log.warn("[SKIP] 필수 파일 누락 → Job 정상 종료 처리됨: {}, {}", input, touch);
//            throw new IllegalStateException("필수 입력 파일이 존재하지 않습니다: " + input + " / " + touch);
        } else {
            log.info("✔ 입력 파일 존재 확인 완료: {}, {}", input, touch);
            // 매니저 이름 조회
            String manager2 = query.selectManager("PRESIDENT");

            // ExecutionContext에 저장
            stepExecution.getExecutionContext().put("manager2", manager2);
            log.info("✔ 매니저 정보 저장 완료: {}", manager2);

        }
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.debug("[InputFileCheckListener]  afterStep ======");
        return stepExecution.getExitStatus();
    }
}
