package com.cetc.hubble.dataquality.plugins.utils;


import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @date 2018/01/03
 * 连接各种数据源
 **/

public class DataSourceGet {

    private static Logger logger = LoggerFactory.getLogger(DataSourceGet.class);



    /**
     *
     * @param spark SparkSession
     * @param jdbcUrl   表的jdbc连接信息
     * @param tableName 表名
     * @param user  用户名
     * @param password 密码
     * @return
     */
    public static Dataset<Row> getDFByJdbc(SparkSession spark,String driver,String jdbcUrl, String tableName, String user, String password){
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("driver",driver)
                .option("url", jdbcUrl)
                .option("dbtable", tableName)
                .option("user", user)
                .option("password", password)
                .load();
        return jdbcDF;
    }

    /**
     *
     * @param spark SparkSession
     * @param index es的index
     * @return
     */
    public static Dataset<Row> getDFByEs(SparkSession spark, String index) {

        Dataset<Row> esDF = spark.read().format("org.elasticsearch.spark.sql")
                .option("es.nodes", "").option("es.port", "")
                .option("es.index.auto.create", "true")
                .load(index);
        return esDF;

    }


    /**
     *
     * @param spark
     * @param inputDBType
     * @param inputIp
     * @param inputPort
     * @param inputDbName
     * @param inputTable
     * @param inputUserName
     * @param inputPassword
     * @return
     */
    public static Dataset<Row> readTable(SparkSession spark,String inputDBType,String inputIp,String inputPort,
                                         String inputDbName,String inputTable,String inputUserName,String inputPassword) {

        Dataset<Row> dataSet=null;
        if("MYSQL".equalsIgnoreCase(inputDBType)){

            String jdbcUrl=new StringBuffer("jdbc:mysql://").append(inputIp).append(":").append(inputPort).append("/").append(inputDbName).toString();
            String driver = "com.mysql.jdbc.Driver";
            dataSet = getDFByJdbc(spark,driver,jdbcUrl,inputTable,inputUserName,inputPassword);

        }else if("ORACLE".equalsIgnoreCase(inputDBType)){
            String jdbcUrl=new StringBuffer("jdbc:oracle:thin:@").append(inputIp).append(":").append(inputPort).append(":").append(inputDbName).toString();
            String driver = "oracle.jdbc.driver.OracleDriver";
            dataSet = getDFByJdbc(spark,driver,jdbcUrl,inputTable,inputUserName,inputPassword);

        } else if("GREENPLUM".equalsIgnoreCase(inputDBType)){

            String jdbcUrl=new StringBuffer("jdbc:postgresql://").append(inputIp).append(":").append(inputPort).append("/").append(inputDbName).toString();
            String driver = "org.postgresql.Driver";
            dataSet = getDFByJdbc(spark,driver,jdbcUrl,inputTable,inputUserName,inputPassword);

        } else if("HIVE2".equals(inputDBType)){
            dataSet = spark.sql("select * from "+inputTable);
        } else {
            System.out.println("没有匹配的类型");
        }

        logger.info("======"+dataSet.count());

        return dataSet;

    }


}
