package com.boddi.honeycomb.sparkbee.repository;

import com.boddi.honeycomb.sparkbee.entity.TableConfig;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by guoyubo on 2017/6/2.
 */
public class TableConfigRepository {

  public static final DataSource CONFIG_DATA_SOURCE = DataSourceFactory.getConfigDataSource();

  private  Logger logger = LoggerFactory.getLogger(this.getClass());

  public List<TableConfig> getTableConfigsByBatchId(Long batchId) {
    QueryRunner queryRunner = new QueryRunner(CONFIG_DATA_SOURCE);
    try {
      return queryRunner.query(
          "select id,BATCH_ID,FROM_TABLE_NAME,TABLE_OWNER,PRIMARY_KEY,"
              + "TO_TABLE_NAME,MERGE_TYPE,JNDI_NAME "
              + "from ETL_TABLE_CONF where batch_id=?",
          new ResultSetHandler< List<TableConfig>>() {
            @Override
            public  List<TableConfig> handle(final ResultSet rs) throws SQLException {
              List<TableConfig> tableConfigs = new ArrayList<>();
              while (rs.next()) {
                TableConfig tableConfig = new TableConfig();
                tableConfig.setId(rs.getLong(1));
                tableConfig.setBatchId(rs.getLong(2));
                tableConfig.setFromTableName(rs.getString(3));
                tableConfig.setTableOwner(rs.getString(4));
                tableConfig.setPrimaryKey(rs.getString(5));
                tableConfig.setToTableName(rs.getString(6));
                tableConfig.setMergeType(rs.getString(7));
                tableConfig.setJndiName(rs.getString(8));
                tableConfigs.add(tableConfig);
              }
              return tableConfigs;
            }
          }, batchId);
    } catch (SQLException e) {
       logger.error("query table configs error", e);
    } finally {
    }

    return null;
  }
}
