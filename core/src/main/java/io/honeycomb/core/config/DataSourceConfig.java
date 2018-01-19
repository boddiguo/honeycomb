package io.honeycomb.core.config;

import org.apache.ibatis.type.JdbcType;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.mapper.MapperScannerConfigurer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;

import java.beans.PropertyVetoException;

/**
 * Created by guoyubo on 2017/7/30.
 */
@Configuration
public class DataSourceConfig implements EnvironmentAware {

  @Autowired
  public Environment env;

  @Autowired
  public DataSourceProperties dataSourceProperties;


  @Bean(name="dataSource")
  public DataSource dataSource() throws PropertyVetoException {
    DruidDataSource dataSource = new DruidDataSource();
    dataSource.setDriverClassName(env.getProperty("datasource.driverClassName"));
    dataSource.setUrl(env.getProperty("datasource.url"));
    dataSource.setUsername(env.getProperty("datasource.username"));
    dataSource.setPassword(env.getProperty("datasource.password"));
    dataSource.setMaxWait(6000);
    dataSource.setMaxActive(15);
    return dataSource;
  }


  @Bean(name = "sqlSessionFactory")
  public SqlSessionFactoryBean sqlSessionFactory(@Autowired DataSource dataSource, ApplicationContext applicationContext) throws Exception {
    SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
    sessionFactory.setDataSource(dataSource);

    org.apache.ibatis.session.Configuration configuration = new org.apache.ibatis.session.Configuration();
    configuration.setMapUnderscoreToCamelCase(true);
    configuration.setJdbcTypeForNull(JdbcType.NULL);
    sessionFactory.setConfiguration(configuration);
    sessionFactory.setMapperLocations(applicationContext.getResources("classpath:mapper/*.xml"));


    return sessionFactory;
  }

  @Bean
  public MapperScannerConfigurer MapperScannerConfigurer() {
    MapperScannerConfigurer mapperScannerConfigurer = new MapperScannerConfigurer();
    mapperScannerConfigurer.setBasePackage("io.honeycomb.core.mapper");
    mapperScannerConfigurer.setSqlSessionFactoryBeanName("sqlSessionFactory");
    return mapperScannerConfigurer;
  }

  @Bean(name = "transactionManager")
  public PlatformTransactionManager annotationDrivenTransactionManager(@Autowired DataSource dataSource) {
    return new DataSourceTransactionManager(dataSource);
  }

  @Override
  public void setEnvironment(final Environment environment) {
    this.env = environment;
  }
}
