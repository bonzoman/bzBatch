package com.bzbatch.sample.dto;

import lombok.Data;
import org.springframework.batch.item.Chunk;

@Data
public class AutoBatchCommonDto extends Chunk<AutoBatchCommonDto> {
    String commonString;
}
