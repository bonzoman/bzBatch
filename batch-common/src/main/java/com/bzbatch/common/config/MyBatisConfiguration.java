package com.bzbatch.common.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
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

    @Bean(name = "defaultSqlSessionFactory")
    @Primary
    public SqlSessionFactory defaultSqlSessionFactory(@Qualifier("defaultDataSource") DataSource dataSource) throws Exception {
        log.debug("== defaultSqlSessionFactory ==");
        return getDefaultSqlSessionFactory(dataSource);
    }

    @Bean(name = "manualSqlSessionFactory")
    public SqlSessionFactory manualSqlSessionFactory(@Qualifier("defaultDataSource") DataSource dataSource) throws Exception {
        log.debug("== manualSqlSessionFactory 트랜젝션 수동제어용 ==");
        return getManualSqlSessionFactory(dataSource);
    }

    @Bean(name = "hrSqlSessionFactory")
    public SqlSessionFactory hrSqlSessionFactory(@Qualifier("hrDataSource") DataSource dataSource) throws Exception {
        log.debug("== hrSqlSessionFactory ==");
        return getDefaultSqlSessionFactory(dataSource);
    }

    private SqlSessionFactory getDefaultSqlSessionFactory(DataSource dataSource) throws Exception {
        SqlSessionFactoryBean factoryBean = new SqlSessionFactoryBean();
        factoryBean.setDataSource(dataSource);
        factoryBean.setMapperLocations(new PathMatchingResourcePatternResolver().getResources(this.mapperLocations));
        return factoryBean.getObject();
    }

    private SqlSessionFactory getManualSqlSessionFactory(DataSource dataSource) throws Exception {
        SqlSessionFactoryBean factoryBean = new SqlSessionFactoryBean();
        factoryBean.setDataSource(dataSource);
        factoryBean.setMapperLocations(new PathMatchingResourcePatternResolver().getResources(this.mapperLocations));

        //NOTE: 트랜잭션 autoCommit = false 적용되게...
        // - 적용하지 않으면 기본 SpringManagedTransactionFactory 를 사용하게된다.
        factoryBean.setTransactionFactory(new JdbcTransactionFactory());

        return factoryBean.getObject();
    }

    // SqlSessionTemplate 등록
//    @Bean
//    @Primary
//    public SqlSessionTemplate defaultSqlSessionTemplate(@Qualifier("defaultSqlSessionFactory") SqlSessionFactory sqlSessionFactory) {
//        log.debug("== defaultSqlSessionTemplate ==");
//        return new SqlSessionTemplate(sqlSessionFactory);
//    }
//
//    @Bean
//    public SqlSessionTemplate hrSqlSessionTemplate(@Qualifier("hrSqlSessionFactory") SqlSessionFactory sqlSessionFactory) {
//        log.debug("== hrSqlSessionTemplate ==");
//        return new SqlSessionTemplate(sqlSessionFactory);
//    }
}