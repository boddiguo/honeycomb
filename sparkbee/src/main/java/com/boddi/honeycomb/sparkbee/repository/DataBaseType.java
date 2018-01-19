package com.boddi.honeycomb.sparkbee.repository;

public enum DataBaseType {
    MySql("mysql", "com.mysql.jdbc.Driver"),
    Oracle("oracle", "oracle.jdbc.OracleDriver"),
    Hive("hive", "org.apache.hive.jdbc.HiveDriver"),
    Impala("impala", "org.apache.hive.jdbc.HiveDriver");

    private String typeName;
    private String driverClassName;


    DataBaseType(String typeName, String driverClassName) {
        this.typeName = typeName;
        this.driverClassName = driverClassName;
    }

    public String getDriverClassName() {
        return this.driverClassName;
    }

    public String getTypeName() {
        return typeName;
    }



}
