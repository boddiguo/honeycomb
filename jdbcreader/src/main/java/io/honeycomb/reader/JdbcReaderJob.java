package io.honeycomb.reader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import io.honeycomb.common.model.ReaderJobConfig;
import io.honeycomb.common.exception.HoneyCombException;
import io.honeycomb.common.job.AbstractReaderJob;
import io.honeycomb.reader.config.DatabaseConfig;
import io.honeycomb.reader.config.TableConfig;
import io.honeycomb.reader.datax.DataXEngine;
import io.honeycomb.reader.util.DBUtil;
import io.honeycomb.reader.util.DBUtilErrorCode;
import io.honeycomb.reader.util.DataBaseType;

/*
 * Created by guoyubo on 2018/1/10.
 */
public class JdbcReaderJob extends AbstractReaderJob {

  private DatabaseConfig databaseConfig;
  private TableConfig tableConfig;

  public void init() {
    databaseConfig = JSON.parseObject(readerJobConfig.getSourceConfig(), DatabaseConfig.class);
    tableConfig = JSON.parseObject(readerJobConfig.getItemConfig(), TableConfig.class);
  }

  public void pre() {

    if (databaseConfig.getDatabaseType() == null) {
      throw new HoneyCombException("databaseType is null");
    }

    if (tableConfig.getFromTableName() == null) {
      throw new HoneyCombException("fromTable is null");
    }

    if (tableConfig.getExtractColumns() == null) {
      throw new HoneyCombException("extract Columns is null");
    }

  }

  public boolean run() {

    DataBaseType dataBaseType = DataBaseType.valueOf(databaseConfig.getDatabaseType());

    String lastExtractValue = tableConfig.getLastExtractValue();

    String extractValueType = tableConfig.getExtractValueType();

    StringBuffer condition = new StringBuffer();

    if (tableConfig.getExtractKey() != null && lastExtractValue != null) {
      condition.append(tableConfig.getExtractKey());
       condition.append(">=");
      if (extractValueType == "data" && dataBaseType.equals(DataBaseType.Oracle)) {
        condition.append("to_date('");
        condition.append(lastExtractValue);
        condition.append("','");
        condition.append(extractValueType);
        condition.append("')");
      } else {
        condition.append("'");
        condition.append(lastExtractValue);
        condition.append("'");
      }
    } else {
        condition.append("1=1");
    }
    String querySql =
        String.format(" select %s from %s where %s",
            tableConfig.getExtractColumns(),
            tableConfig.getFromTableName().toUpperCase(),
            condition.toString()
        );

    JSONObject allObject = new JSONObject();
    JSONObject jobObject = new JSONObject();
    JSONArray contentArray = new JSONArray();
    JSONObject contentObject = new JSONObject();
    JSONObject readerObject = buildReaderObject(databaseConfig, dataBaseType, querySql);
    contentObject.put("reader", readerObject);

    Connection connection = DBUtil
        .getConnection(dataBaseType, databaseConfig.getJdbcUrl(),
            databaseConfig.getUserName(), databaseConfig.getPassword());
    try {

      ResultSet resultSet = searchFirstResultSet(querySql, dataBaseType, connection);
      JSONObject writerObject = buildWriterObject(readerJobConfig, tableConfig, resultSet);

      contentObject.put("writer", writerObject);
      contentArray.add(contentObject);
      jobObject.put("content", contentArray);

      JSONObject settingObject = new JSONObject();
      final JSONObject speedObject = new JSONObject();
      speedObject.put("channel", 10);
      settingObject.put("speed", speedObject);
      jobObject.put("setting", settingObject);

      allObject.put("job", jobObject);

      String result = DataXEngine.instance.execute(allObject.toJSONString(), 1l);

      return true;
    } catch (SQLException e) {
      throw new HoneyCombException(DBUtilErrorCode.SQL_EXECUTE_FAIL, "执行sql报错:" + querySql, e);
    } finally {
      try {
        connection.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

  }

  @Override
  public boolean cancel() {
    return false;
  }

  private ResultSet searchFirstResultSet(final String querySql, final DataBaseType dataBaseType,
      final Connection connection) throws SQLException {
    StringBuffer pagingSelect = new StringBuffer( querySql.length()+100 );

    pagingSelect.append("select row_.* from ( ");
    pagingSelect.append(querySql);
    if (dataBaseType.equals(DataBaseType.Oracle)) {
      pagingSelect.append(" ) row_ where rownum=1");

    } else {
      pagingSelect.append(" ) row_  limit 1");
    }
    return DBUtil.query(connection, pagingSelect.toString());
  }

  private JSONObject buildWriterObject(final ReaderJobConfig readerJobConfig, final TableConfig tableConfig,
      final ResultSet resultSet) throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    List<String> columns = Lists.newArrayList();
    int columnCount = metaData.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      columns.add(metaData.getColumnName(i));
    }
    JSONObject parameterObject = new JSONObject();
    parameterObject.put("topic", "datax.db.sync");
    parameterObject.put("toTableName", tableConfig.getToTableName());
    parameterObject.put("column", columns);
    JSONArray connectionArray = new JSONArray();
    JSONObject connectionObject = new JSONObject();
    connectionObject.put("serverAddress", readerJobConfig.getKafkaServerAddress());
    connectionArray.add(connectionObject);
    parameterObject.put("connection", connectionArray);
    JSONObject writerObject = new JSONObject();
    writerObject.put("name", "kafkawriter");
    writerObject.put("parameter", parameterObject);
    return writerObject;
  }

  private JSONObject buildReaderObject(final DatabaseConfig databaseConfig,
      final DataBaseType dataBaseType, final String querySql) {
    JSONObject readerObject = new JSONObject();
    JSONObject parameterObject = new JSONObject();
    readerObject.put("name", dataBaseType.getReaderName());
    parameterObject.put("username", databaseConfig.getUserName());
    parameterObject.put("password", databaseConfig.getPassword());
    JSONArray connectionArray = new JSONArray();
    JSONObject connectionObject = new JSONObject();
    connectionObject.put("jdbcUrl", Lists.newArrayList(databaseConfig.getJdbcUrl()));
    connectionObject.put("querySql", Lists.newArrayList(querySql));
    connectionArray.add(connectionObject);

    parameterObject.put("connection", connectionArray);

    readerObject.put("parameter", parameterObject);
    return readerObject;
  }

  public void post() {

  }

  public void destroy() {

  }

  public static void main(String[] args) throws SQLException {
    ReaderJobConfig readerJobConfig = new ReaderJobConfig();
    readerJobConfig.setKafkaServerAddress("localhost:9092");
    readerJobConfig.setName("db sync");
    DatabaseConfig databaseConfig = new DatabaseConfig();
    databaseConfig.setDatabaseType(DataBaseType.Oracle.name());
    databaseConfig.setJdbcUrl("jdbc:oracle:thin:@10.18.19.33:1521:dbdev2");
    databaseConfig.setUserName("sl_main");
    databaseConfig.setPassword("sl_main");
    readerJobConfig.setSourceConfig(JSON.toJSONString(databaseConfig));

    final TableConfig itemConfig = new TableConfig();
    itemConfig.setFromTableName("SL$cfg");
    itemConfig.setToTableName("cfg");
    itemConfig.setExtractColumns("*");
    readerJobConfig.setItemConfig(JSON.toJSONString(itemConfig));
    JdbcReaderJob jdbcReaderJob = new JdbcReaderJob();
    jdbcReaderJob.setReaderJobConfig(readerJobConfig);
    jdbcReaderJob.init();
    jdbcReaderJob.run();


  }

}


