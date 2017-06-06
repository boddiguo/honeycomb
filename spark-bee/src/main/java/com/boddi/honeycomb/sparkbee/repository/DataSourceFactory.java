package com.boddi.honeycomb.sparkbee.repository;

import org.apache.commons.collections.map.DefaultedMap;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by guoyubo on 2017/6/2.
 */
public class DataSourceFactory {

  private static Map<String, DataSource> dataSourceMap = new HashMap<>();


  public static DataSource getDataSource(DataBaseType dataBaseType,
      String url, String userName, String password) {
    String key = dataBaseType + "-" + url + "-" + userName;
    if (!dataSourceMap.containsKey(key)) {
      DruidDataSource dataSource = new DruidDataSource();
      dataSource.setDbType(dataBaseType.getTypeName());
      dataSource.setDriverClassName(dataBaseType.getDriverClassName());
      dataSource.setUrl(url);
      dataSource.setUsername(userName);
      dataSource.setPassword(password);
      dataSourceMap.put(key, dataSource);
    }

    return dataSourceMap.get(key);
  }


  public static DataSource getConfigDataSource() {
    return getDataSource(DataBaseType.MySql,
        "jdbc:mysql://127.0.0.1:3306/etl_config", "root", "123456");
  }

}
