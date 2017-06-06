package com.boddi.honeycomb.canalparser;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.context.annotation.PropertySource;

/**
 * Created by Zhan Qing on 3/25/16.
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class,
    DataSourceTransactionManagerAutoConfiguration.class, BatchAutoConfiguration.class},
                       scanBasePackages = {Application.BASE_PACKAGE, Application.COR_BASE_PACKAGE,
                           Application.BASE_PACKAGE})
@PropertySource(value = {"application.yml"})
public class Application {
  public static final String BASE_PACKAGE = "com.boddi.honeycomb.canalparser";
  public static final String COR_BASE_PACKAGE = "com.boddi.honeycomb";

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }
}
