package io.honeycomb.reader.config;

import lombok.Data;

/**
 * Created by guoyubo on 2018/1/10.
 */
@Data
public class DatabaseConfig {


  private String databaseType;

  private String jdbcUrl;

  private String userName;

  private String password;



}
