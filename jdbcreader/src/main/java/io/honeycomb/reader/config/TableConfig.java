package io.honeycomb.reader.config;

import lombok.Data;

/**
 * Created by guoyubo on 2018/1/10.
 */
@Data
public class TableConfig {

  /**
   * 主键 PRIMARY_KEY
   */
  private String primaryKey;

  private String fromTableName;

  private String toTableName;

  private String extractKey;

  private String extractColumns;

  private String lastExtractValue;

  private String extractValueType;





}
