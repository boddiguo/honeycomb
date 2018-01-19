package io.honeycomb.core.entity;

import lombok.Data;

/**
 * Created by guoyubo on 2018/1/10.
 */
@Data
public class WriterItem extends BaseEntity {

  private long writerComponentId;


  private String name;


  private String itemConfig;


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
   * 分区字段 PARTITION_KEY
   */
  private String partitionKey;







}
