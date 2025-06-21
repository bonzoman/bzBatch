package com.bzbatch.common;

public interface BatchDbCallback<T> {

    /**
     * DB 로직 수행 (insert, update 등)
     */
    void doInSession(T item, Object mapper) throws Exception;

//    /**
//     * 트랜잭션 커밋 후 호출 (성공 시점에만)
//     * - 여기서 최종 카운터 증가 처리
//     */
//    default boolean onSuccess(T item) {
//        return true;
//    }
}
