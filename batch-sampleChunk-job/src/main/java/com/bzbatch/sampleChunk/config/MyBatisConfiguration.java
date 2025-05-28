package com.bzbatch.sampleChunk.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import javax.sql.DataSource;

@Slf4j
@Configuration
public class MyBatisConfiguration {

    @Value("${mybatis.mapper-locations}")
    private String mapperLocations;

    @Bean
    @Primary
    public SqlSessionFactory defaultSqlSessionFactory(@Qualifier("defaultDataSource") DataSource dataSource) throws Exception {
        log.debug("== defaultSqlSessionFactory ==");
        return getSqlSessionFactory(dataSource);
    }

    @Bean
    public SqlSessionFactory hrSqlSessionFactory(@Qualifier("hrDataSource") DataSource dataSource) throws Exception {
        log.debug("== hrSqlSessionFactory ==");
        return getSqlSessionFactory(dataSource);
    }

    private SqlSessionFactory getSqlSessionFactory(DataSource dataSource) throws Exception {
        SqlSessionFactoryBean factoryBean = new SqlSessionFactoryBean();
        factoryBean.setDataSource(dataSource);
        factoryBean.setMapperLocations(new PathMatchingResourcePatternResolver().getResources(this.mapperLocations));
        return factoryBean.getObject();
    }
}