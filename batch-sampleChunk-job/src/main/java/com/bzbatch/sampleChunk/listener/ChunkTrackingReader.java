package com.bzbatch.sampleChunk.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ChunkTrackingReader<T> implements ItemReader<T>, ItemStream {

    private final ItemReader<T> reader;
    private final List<T> currentChunk = new ArrayList<>();
    private int count = 0;

    public ChunkTrackingReader(ItemReader<T> reader) {
        this.reader = reader;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        log.info("▣▣▣▣▣▣▣▣▣▣ chunkTrackingReader.open(1)");
//        if (reader instanceof ItemStream) {
//            ((ItemStream) reader).open(executionContext);
//        }
    }

    @Override
    public void update(ExecutionContext executionContext) {
        log.info("▣▣▣▣▣▣▣▣▣▣ chunkTrackingReader.update(2)");
//        try {
//            T item = reader.read();
//            log.info("▣▣▣▣▣▣▣▣▣▣ chunkTrackingReader.update(2) [{}번째 처리] 대상: {}", count, item);
//        } catch (Exception e) {
//        }
//        executionContext.put("chunk.items", new ArrayList<>(currentChunk));
    }

    @Override
    public T read() throws Exception {
        count++;
        T item = reader.read();
        log.info("▣▣▣▣▣▣▣▣▣▣ chunkTrackingReader.read(3) [{}번째 처리] 대상: {}", count, item);
        if (item != null) {
            currentChunk.add(item);
        } else if (!currentChunk.isEmpty()) {
            log.debug("✔ read 종료 후 currentChunk 초기화");
            currentChunk.clear(); // 다음 chunk 대비
        }
        return item;
    }

    public List<T> getCurrentChunk() {
        return currentChunk;
    }


    @Override
    public void close() {
        log.info("▣▣▣▣▣▣▣▣▣▣ chunkTrackingReader.close");
    }
}
