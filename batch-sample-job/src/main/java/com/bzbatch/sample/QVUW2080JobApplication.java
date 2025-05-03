package com.bzbatch.sample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages = {
        "com.bzbatch.common.config",
        "com.bzbatch.sample"
})
@SpringBootApplication
public class QVUW2080JobApplication {

    public static void main(String[] args) {
        SpringApplication.run(QVUW2080JobApplication.class, args);
    }

}
