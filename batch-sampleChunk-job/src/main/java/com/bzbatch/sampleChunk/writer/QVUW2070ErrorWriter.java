package com.bzbatch.sampleChunk.writer;


import com.bzbatch.sampleChunk.dto.AutoBatchCommonDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.core.io.FileSystemResource;

@Slf4j
@RequiredArgsConstructor
public class QVUW2070ErrorWriter implements SkipListener<Object, Object> {

    private final String date;
    private final String time;
    private FlatFileItemWriter<AutoBatchCommonDto> delegate;

    @Override
    public void onSkipInProcess(Object item, Throwable t) {
        try {
            if (delegate == null) initWriter();
            AutoBatchCommonDto dto = new AutoBatchCommonDto();
            dto.setCommonString("실패: " + item.toString());
            delegate.write(new org.springframework.batch.item.Chunk<>(dto));
        } catch (Exception e) {
            log.error("[에러 로그 쓰기 실패]", e);
        }
    }

    private void initWriter() throws Exception {
        delegate = new FlatFileItemWriterBuilder<AutoBatchCommonDto>()
                .name("errorWriter")
                .resource(new FileSystemResource("/batchlog/ZU2080." + date + "." + time + ".ERR.OUT"))
                .encoding("EUC-KR")
                .delimited().delimiter("^")
                .names("commonString")
                .build();
        delegate.afterPropertiesSet();
        delegate.open(new org.springframework.batch.item.ExecutionContext());
    }

    @Override
    public void onSkipInRead(Throwable t) {
    }

    @Override
    public void onSkipInWrite(Object item, Throwable t) {
    }
}