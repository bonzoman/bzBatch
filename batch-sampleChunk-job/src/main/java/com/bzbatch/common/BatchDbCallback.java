package com.bzbatch.common;

public interface BatchDbCallback<T> {
    void doInSession(T item, Object mapper) throws Exception;
}
