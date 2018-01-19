package io.honeycomb.core.service;

import org.springframework.stereotype.Service;

import io.honeycomb.core.entity.TableSyncLog;

/**
 * Created by guoyubo on 2018/1/16.
 */
@Service
public class TableSyncLogService {


  public TableSyncLog getLastestSyncLog(String tableName) {
    return new TableSyncLog();
  }

}
