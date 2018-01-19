package io.honeycomb.core.entity;

import lombok.Data;

/**
 * Created by guoyubo on 2018/1/10.
 */
@Data
public class ReaderItem extends BaseEntity {

  private int sourceId;

  private String name;

  /**
   * DB: {"fromTableName": "user", "primaryKey": "id", "updatedKey": "updated_at", "cronExpression":"0 0 0 * * ?"}
   * Canal:
   */
  private String itemConfig;


  private Integer status;



}
