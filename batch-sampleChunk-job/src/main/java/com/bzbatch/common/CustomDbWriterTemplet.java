package com.bzbatch.common;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.util.List;
import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class CustomDbWriterTemplet<T> {

    private final SqlSessionFactory sqlSessionFactory;

    public void execute(List<? extends T> items,
                        Class<?> mapperClass,
                        boolean useBatch,
                        BatchDbCallback<T> callback,
                        Consumer<Integer> onCommit) {//Consumer : in 1개, out없음

        ExecutorType executorType = useBatch ? ExecutorType.BATCH : ExecutorType.SIMPLE;
        SqlSession session = null;

        try {
            session = sqlSessionFactory.openSession(executorType, false);
            Object mapper = session.getMapper(mapperClass);

            if (useBatch) {
                //ExecutorType.BATCH ---------------------------------
                for (T item : items) {
                    callback.doInSession(item, mapper);
                }
                session.flushStatements();
                session.commit();
                // commit 성공 → call the consumer
                onCommit.accept(1); // ✅ 단순히 commit 발생 사실만 알림 (count는 아님)


//                // BATCH 성공 시: 개별 항목에 대해 후처리 실행
//                int success = 0;
//                for (T item : items) {
//                    if (callback.onSuccess(item)) {
//                        success++;
//                    }
//                }
//                onSuccessCount.accept(success);

            } else {
                //ExecutorType.SIMPLE ---------------------------------
                for (T item : items) {
                    try {
                        callback.doInSession(item, mapper);
                        session.commit();
                        onCommit.accept(1); // ✅ 단순히 commit 발생 사실만 알림 (count는 아님)
                    } catch (Exception ex) {
                        log.error("단건 처리 실패: {}", item, ex);
                        session.rollback();
                    }
                }
            }
        } catch (Exception e) {
            ///////////////////////////////////////////////////////////////////////////////////////////////////////////
            // 2차: BATCH 실패 시, 단건 처리로 재시도
            ///////////////////////////////////////////////////////////////////////////////////////////////////////////
            if (useBatch) {
                log.warn("예외 발생 → rollback", e);
                if (session != null) {
                    try {
                        session.rollback(); // rollback
                    } catch (Exception rollbackEx) {
                        log.error("rollback 중 예외", rollbackEx);
                    }
                }

                log.warn("예외 발생: fallback 단건 처리 시작", e);

                // BATCH 실패 시 단건 처리 fallback
                try (SqlSession fallbackSession = sqlSessionFactory.openSession(ExecutorType.SIMPLE, false)) {
                    Object fallbackMapper = fallbackSession.getMapper(mapperClass);

                    for (T item : items) {
                        try {
                            callback.doInSession(item, fallbackMapper);
                            fallbackSession.commit();
                            onCommit.accept(1); // ✅ 단순히 commit 발생 사실만 알림 (count는 아님)
                        } catch (Exception ex) {
                            log.error("단건 처리 실패: {}", item, ex);
                            fallbackSession.rollback();
                        }
                    }
                } catch (Exception ex) {
                    log.error("fallback 세션 처리 중 오류", ex);
                }
            }
        } finally {
            if (session != null) {
                session.close(); // 꼭 닫기
            }
        }
    }
}
