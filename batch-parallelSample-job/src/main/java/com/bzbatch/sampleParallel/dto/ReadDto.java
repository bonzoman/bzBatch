package com.bzbatch.sampleParallel.dto;

import lombok.Data;

@Data
public class ReadDto {
    String startId;
    String endId;
    int perCntId;
    int totCntId;
}
