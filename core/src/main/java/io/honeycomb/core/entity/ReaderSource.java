package io.honeycomb.core.entity;

import lombok.Data;

/**
 * Created by guoyubo on 2018/1/10.
 */
@Data
public class ReaderSource extends BaseEntity {

  private String type;

  private String config;

  private String kafkaServerAddress;

}
