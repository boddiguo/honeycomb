package io.honeycomb.core;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableAutoConfiguration(exclude =
    { DataSourceTransactionManagerAutoConfiguration.class,
        DataSourceAutoConfiguration.class })
@ComponentScan(basePackages ="io.honeycomb.core")
@EnableTransactionManagement
public class SimpleApplication {



}
