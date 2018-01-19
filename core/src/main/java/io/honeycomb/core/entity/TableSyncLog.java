package io.honeycomb.core.entity;

import java.util.Date;

import lombok.Data;

/**
 * Created by guoyubo on 2018/1/12.
 */
@Data
public class TableSyncLog extends BaseEntity {

  private String tableName;

  private String extractKey;

  private String extractValue;

  private String extractValueType;

  private Date startDate;

  private Date endDate;


}
