package io.honeycomb.common.model;

import lombok.Data;

/**
 * Created by guoyubo on 2018/1/10.
 */
@Data
public class ReaderJobConfig {



  private String name;

  private String type;

  /**
   * DB: {"fromTableName": "user", "primaryKey": "id", "extractKey": "updated_at"}
   *
   */
  private String itemConfig;


  private String sourceConfig;


  private String kafkaServerAddress;



}
