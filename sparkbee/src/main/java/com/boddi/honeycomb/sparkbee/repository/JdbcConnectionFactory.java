package com.boddi.honeycomb.sparkbee.repository;


import com.boddi.honeycomb.sparkbee.exception.EtlException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * 连接工厂
 */
public class JdbcConnectionFactory {

    private DataBaseType dataBaseType;

    private final String jdbcUrl;

    private final String userName;

    private final String password;

    public JdbcConnectionFactory(DataBaseType dataBaseType, String jdbcUrl, String userName, String password) {
        this.dataBaseType = dataBaseType;
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    public Connection getConnection() {
        try {
            return connect(dataBaseType, jdbcUrl, userName,
                password);
        } catch (Exception e) {
            throw new EtlException(e);
        }

    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
                                                   String url, String user, String pass) {
        Properties prop = new Properties();
        if (dataBaseType == DataBaseType.Hive || dataBaseType == DataBaseType.Impala) {
            return connect(dataBaseType, url, prop);
        }

        prop.put("user", user);
        prop.put("password", pass);


        return connect(dataBaseType, url, prop);
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
                                                   String url, Properties prop) {
        try {
            Class.forName(dataBaseType.getDriverClassName());
            return DriverManager.getConnection(url, prop);
        } catch (Exception e) {
            throw new EtlException(e);
        }
    }



}
