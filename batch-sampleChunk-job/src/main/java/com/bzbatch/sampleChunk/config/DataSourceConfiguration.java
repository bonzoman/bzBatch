package com.bzbatch.sampleChunk.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;


@Configuration
public class DataSourceConfiguration {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    @Bean(name = "defaultDataSource")
    @Primary
    @ConfigurationProperties(prefix = "bzbatch.datasource.orasfqt-ssfqgvut-g2m")
    public DataSource defaultDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "defaultTransactionManager")
    @Primary
    public PlatformTransactionManager defaultTransactionManager(@Qualifier("defaultDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    @Bean(name = "hrDataSource")
    @ConfigurationProperties(prefix = "bzbatch.datasource.orasfqt-ssfqgvut-hr")
    public DataSource hrDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "hrTransactionManager")
    public PlatformTransactionManager hrTransactionManager(@Qualifier("hrDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

}