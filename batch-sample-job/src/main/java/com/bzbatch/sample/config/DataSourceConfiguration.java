package com.bzbatch.sample.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;


@Configuration
public class DataSourceConfiguration {
    @Bean(name = "defaultDataSource")
//    @Primary
    @ConfigurationProperties(prefix = "bzbatch.datasource.orasfqt-ssfqgvut")
//    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource defaultDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "defaultTransactionManager")
    public PlatformTransactionManager defaultTransactionManager(@Qualifier("defaultDataSource") DataSource defaultDataSource) {
        return new DataSourceTransactionManager(defaultDataSource);
    }
}