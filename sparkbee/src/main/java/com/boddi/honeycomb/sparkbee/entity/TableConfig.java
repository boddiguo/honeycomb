package com.boddi.honeycomb.sparkbee.entity;

import java.util.Date;

import lombok.Data;
import lombok.ToString;

/**
 * Created by guoyubo on 2017/6/2.
 */
@Data
@ToString
public class TableConfig {

  /**
   * 主键
   */
  private Long id;
  /**
   * 原始表 FROM_TABLE_NAME
   */
  private String fromTableName;
  /**
   * owner TABLE_OWNER
   */
  private String tableOwner;
  /**
   * 主键 PRIMARY_KEY
   */
  private String primaryKey;
  /**
   * hive表名 TO_TABLE_NAME
   */
  private String toTableName;
  /**
   * oracle链接jdbc名 JNDI_NAME
   */
  private String jndiName;
  /**
   * 最后一次抽取数据时间 LAST_EXTRACT_TIME
   */


  /**
   * 拉链表还是流水表  MERGE_TYPE，CHAIN OR TRANS
   */
  private String mergeType;
  /**
   * 是否合法 Y N   IS_VALID
   */

  private String isValid;

  /**
   * hive表是否存在 Y N 默认是N  PARTITION_TYPE
   */
  private String partitionType;
  /**
   * 默认是*    EXTRACT_COLUMNS
   */
  private String extractColumns;
  /**
   * 批次号  BATCH_ID
   */
  private long batchId;
  /**
   * 更新时间
   */
  private Date updatedAt;
  /**
   * 分区字段 PARTITION_KEY
   */
  private String partitionKey;
  /**
   * 并行度
   */

  private int channel;

  private String isArch;

  private String fromArchTables;

  private int maxArchPartition;



}
