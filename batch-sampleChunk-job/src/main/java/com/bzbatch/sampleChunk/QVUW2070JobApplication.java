package com.bzbatch.sampleChunk;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages = {
        "com.bzbatch"
//        "com.bzbatch.common.config",
//        "com.bzbatch.sampleTasklet"
})
@SpringBootApplication
public class QVUW2070JobApplication {

    public static void main(String[] args) {
        SpringApplication.run(QVUW2070JobApplication.class, args);
    }

}
