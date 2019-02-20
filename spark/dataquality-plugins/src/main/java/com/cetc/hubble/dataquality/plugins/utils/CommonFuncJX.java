package com.cetc.hubble.dataquality.plugins.utils;

import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.*;
import java.util.*;


public class CommonFuncJX {
    private static Logger logger = LoggerFactory.getLogger(CommonFuncJX.class);

    //
    public static Dataset<Row> getDFAllByJdbc(SparkSession spark, String driver, String url, String tableName, String user, String password,Map<String,String> jdbcOptions, String sql) {

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("useSSL", "false")
                .option("driver", driver)
                .option("url", url)
                .option("dbtable", sql)
                .option("user", user)
                .option("password", password)
                .option("numPartitions", Integer.valueOf(jdbcOptions.get("numPartitions")))
                .option("partitionColumn", jdbcOptions.get("partitionColumn"))
                .option("lowerBound", jdbcOptions.get("lowerBound")).option("upperBound", jdbcOptions.get("upperBound"))
                .option("fetchsize", "5000")
                .load();
        return jdbcDF;
    }

    public static Dataset<Row> getDFAfterAggByJdbc(SparkSession spark, String driver, String url, String tableName, String user, String password,String aggSql) {

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("useSSL","false")
                .option("driver", driver)
                .option("url", url)
                .option("dbtable", aggSql)
                .option("user", user)
                .option("password", password)
                .load();
        return jdbcDF;
    }
    public static Dataset<Row> readTable(SparkSession spark,Map<FixedParamEnum,String> linkDBArgs,Map<String,String> jdbcjdbcOptions,
                                         String operation,String sourcedbType) {
        Dataset<Row> dataSet = null;
        String inputDBType = linkDBArgs.get(FixedParamEnum.INPUT_TYPE);
        String inputIp = linkDBArgs.get(FixedParamEnum.INPUT_IP);
        String inputPort = linkDBArgs.get(FixedParamEnum.INPUT_PORT);
        String inputDbName = linkDBArgs.get(FixedParamEnum.INPUT_DB_NAME);
        String inputTable = linkDBArgs.get(FixedParamEnum.INPUT_TABLE);
        String inputUserName = linkDBArgs.get(FixedParamEnum.INPUT_USERNAME);
        String inputPassword = linkDBArgs.get(FixedParamEnum.INPUT_PASSWORD);

        StringBuffer sb = new StringBuffer();
        if ("MYSQL".equalsIgnoreCase(inputDBType)) {

            String jdbcUrl = new StringBuffer("jdbc:mysql://").append(inputIp).append(":").append(inputPort).append("/").append(inputDbName).toString();
            String driver = "com.mysql.jdbc.Driver";

            if(!StringUtils.isEmpty(operation) && (operation.equalsIgnoreCase("min") || operation.equalsIgnoreCase("max") || operation.equalsIgnoreCase("count")) ){
                String aggSql = sb.append("(select ")
                        .append(operation)
                        .append("(1) from ")
                        .append(inputTable)
                        .append(" where jcsjc = str_to_date('2100-01-01 00:00:00', '%Y-%m-%d %H:%i:%s') ")
                        .append(") AS sourceTable").toString();
                dataSet= getDFAfterAggByJdbc(spark, driver,jdbcUrl, inputTable, inputUserName,inputPassword,aggSql);
            }else{
                String readSql = sb.append("(select @rownum := @rownum +1 AS rownum,t.* from (SELECT  @rownum := 0) r,")
                        .append(inputTable)
                        .append(" t where t.jcsjc = str_to_date('2100-01-01 00:00:00', '%Y-%m-%d %H:%i:%s') order by id ")
                        .append(") AS sourceTable").toString();
                dataSet = getDFAllByJdbc(spark, driver,jdbcUrl, inputTable, inputUserName, inputPassword,jdbcjdbcOptions,readSql);
            }


        } else if ("ORACLE".equalsIgnoreCase(inputDBType)) {

            String jdbcUrl;
            if (sourcedbType.equalsIgnoreCase("SID")) {
                jdbcUrl = new StringBuffer("jdbc:oracle:thin:@").append(inputIp).append(":").append(inputPort).append(":").append(inputDbName).toString();
            } else {
                jdbcUrl = new StringBuffer("jdbc:oracle:thin:@").append(inputIp).append(":").append(inputPort).append("/").append(inputDbName).toString();
            }
            System.out.println("ORACLE链接信息如下： ");
            System.out.println("jdbcUrl : " + jdbcUrl);
            System.out.println("inputUserName : " + inputUserName);
            System.out.println("inputPassword :" + inputPassword);
            String[] tableInfo = inputTable.split("\\.");
            String tableTmp = tableInfo[1];
            String table = "\"" + tableTmp + "\"";
            String schemalTmp = tableInfo[0];
            String schemal = "\"" + schemalTmp + "\"";
            String schemalTable = schemal + "." + table;
            String driver = "oracle.driver.OracleDriver";
            if(!StringUtils.isEmpty(operation) && (operation.equalsIgnoreCase("min") || operation.equalsIgnoreCase("max") || operation.equalsIgnoreCase("count")) ){
                String aggSql = sb.append("(select ")
                        .append(operation)
                        .append("(1) from ")
                        .append(inputTable)
                        .append(" where jcsjc = to_date('2100-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss') ")
                        .append(") AS sourceTable").toString();
                dataSet= getDFAfterAggByJdbc(spark, driver,jdbcUrl, inputTable, inputUserName,inputPassword,aggSql);
            }else{
                String readSql = sb.append("(select rownum,t.* from ")
                        .append(inputTable)
                        .append(" t where t.jcsjc = str_to_date('2100-01-01 00:00:00', '%Y-%m-%d %H:%i:%s') order by id")
                        .append(") AS sourceTable").toString();
                dataSet = getDFAllByJdbc(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword, jdbcjdbcOptions, readSql);
            }


        } else if ("GREENPLUM".equalsIgnoreCase(inputDBType) || "postgresql".equalsIgnoreCase(inputDBType)) {

            String jdbcUrl = new StringBuffer("jdbc:postgresql://").append(inputIp).append(":").append(inputPort).append("/").append(inputDbName).toString();
            System.out.println(inputDBType+"链接信息如下： ");
            System.out.println("jdbcUrl : " + jdbcUrl);
            System.out.println("inputUserName : " + inputUserName);
            System.out.println("inputPassword :" + inputPassword);
            String driver = "org.postgresql.Driver";
            if(!StringUtils.isEmpty(operation) && (operation.equalsIgnoreCase("min") || operation.equalsIgnoreCase("max") || operation.equalsIgnoreCase("count")) ){
                String aggSql = sb.append("(select ")
                        .append(operation)
                        .append("(1) from ")
                        .append(inputTable)
                        .append(" where jcsjc = to_timestamp('2100-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss') ")
                        .append(") AS sourceTable").toString();
                dataSet= getDFAfterAggByJdbc(spark, driver,jdbcUrl, inputTable, inputUserName,inputPassword,aggSql);
            }else{
                String readSql = sb.append("(select ROW_NUMBER() OVER(order by id) AS rownum,t.*  from ")
                        .append(inputTable)
                        .append(" t where t.jcsjc = to_timestamp('2100-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss') ")
                        .append(") AS sourceTable").toString();
                dataSet = getDFAllByJdbc(spark, driver,jdbcUrl, inputTable, inputUserName, inputPassword,jdbcjdbcOptions,readSql);
            }
        } else if ("HIVE2".equalsIgnoreCase(inputDBType)) {
            dataSet = spark.sql("select * from " + inputDbName+"."+inputTable);
        } else if ("HIVE".equalsIgnoreCase(inputDBType)) {
            dataSet = spark.sql("select * from " + inputDbName+"."+inputTable);
        } else if ("DAMENG".equalsIgnoreCase(inputDBType)){
            String jdbcUrl = new StringBuffer("jdbc:dm://").append(inputIp).append(":").append(inputPort).toString();
            logger.info("链接信息如下： jdbcUrl {}, inputUserName {}, inputPassword {}" , jdbcUrl, inputUserName, inputPassword );
            String[] tableInfo = inputTable.split("\\.");
            String tableTmp = tableInfo[1];
            String table = "\"" + tableTmp + "\"";
            String schemalTmp = tableInfo[0];
            String schemal = "\"" + schemalTmp + "\"";
            String schemalTable = schemal + "." + table;
            String driver = "dm.driver.DmDriver";
            if(!StringUtils.isEmpty(operation) && (operation.equalsIgnoreCase("min") || operation.equalsIgnoreCase("max") || operation.equalsIgnoreCase("count")) ){
                //sql语法待确认
                String aggSql=null;
                dataSet= getDFAfterAggByJdbc(spark, driver,jdbcUrl, inputTable, inputUserName,inputPassword,aggSql);
            }else{
                //sql语法待确认
                /*String readSql = sb.append("(select @rownum := @rownum +1 AS rownum,t.* from (SELECT  @rownum := 0) r,")
                        .append(inputTable)
                        .append(" t where t.jcsjc = str_to_date('2100-01-01 00:00:00', '%Y-%m-%d %H:%i:%s') ")
                        .append(") AS sourceTable").toString();*/
                dataSet = getDFAllByJdbc(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword, jdbcjdbcOptions, sb.toString());
            }

        }else {
            logger.info("没有匹配的类型");
        }

        return dataSet;
    }
    public static void writeTable(Dataset dataset, String outDBType, String outIp, String outPort,
                                  String outDbName, String outTable, String outUserName, String outPassword, String sourcedbType,Map<String,String> jdbcOptions) {
        if ("MYSQL".equalsIgnoreCase(outDBType)) {

            String jdbcUrl = new StringBuffer("jdbc:mysql://").append(outIp).append(":").append(outPort).append("/").append(outDbName).toString();

            String driver = "com.mysql.jdbc.Driver";
            writeDFToTable(dataset, jdbcUrl, outTable, driver, outUserName, outPassword,jdbcOptions,outDBType);

        } else if ("ORACLE".equalsIgnoreCase(outDBType)) {

            String jdbcUrl;
            if (sourcedbType.equalsIgnoreCase("SID")) {
                jdbcUrl = new StringBuffer("jdbc:oracle:thin:@").append(outIp).append(":").append(outPort).append(":").append(outDbName).toString();
            } else {
                jdbcUrl = new StringBuffer("jdbc:oracle:thin:@").append(outIp).append(":").append(outPort).append("/").append(outDbName).toString();
            }
            String[] tableInfo = outTable.split("\\.");
            String tableTmp = tableInfo[1];
            String table = "\"" + tableTmp + "\"";
            String schemalTmp = tableInfo[0];
            String schemal = "\"" + schemalTmp + "\"";
            String schemalTable = schemal + "." + table;
            String driver = "oracle.jdbc.driver.OracleDriver";
            writeDFToTable(dataset, jdbcUrl, outTable, driver, outUserName, outPassword, jdbcOptions, outDBType);

        } else if ("GREENPLUM".equalsIgnoreCase(outDBType) || "postgresql".equalsIgnoreCase(outDBType)) {

            String jdbcUrl = new StringBuffer("" +
                    "jdbc:postgresql://").append(outIp).append(":").append(outPort).append("/").append(outDbName).toString();
            System.out.println(outDBType+"链接信息如下： ");
            System.out.println("jdbcUrl : " + jdbcUrl);
            System.out.println("outUserName : " + outUserName);
            System.out.println("outPassword :" + outPassword);
            String driver = "org.postgresql.Driver";
            writeDFToTable(dataset, jdbcUrl, outTable, driver, outUserName, outPassword,jdbcOptions,outDBType);

        } else if ("DAMENG".equalsIgnoreCase(outDBType)){
            String jdbcUrl = new StringBuffer("jdbc:dm://").append(outIp).append(":").append(outPort).toString();
            logger.info("链接信息如下： jdbcUrl {}, inputUserName {}, inputPassword {}" , jdbcUrl, outUserName, outPassword );
            String[] tableInfo = outTable.split("\\.");
            String tableTmp = tableInfo[1];
            String table = "\"" + tableTmp + "\"";
            String schemalTmp = tableInfo[0];
            String schemal = "\"" + schemalTmp + "\"";
            String schemalTable = schemal + "." + table;
            String driver = "dm.jdbc.driver.DmDriver";
            writeDFToTable(dataset, jdbcUrl, outTable, driver, outUserName, outPassword,jdbcOptions,outDBType);
        }else {
            logger.info("写入数据时，没有找到匹配的数据库类型");
        }

    }

    public static void writeDFToTable(Dataset dataset,String jdbcUrl, String tableName,String driver ,String outUserName, String outPassword,Map<String,String> jdbcOptions,String dbType){

        Properties prop = new java.util.Properties();
        prop.setProperty("driver",driver );
        prop.setProperty("user", outUserName);
        prop.setProperty("password", outPassword);
        String mode = jdbcOptions.get("saveMode");
        String numPartitions = jdbcOptions.get("numPartitions");
        String primaryKey = jdbcOptions.get("primaryKey");

        if(mode.equalsIgnoreCase("UPDATE")){
            if(dbType.equalsIgnoreCase("MYSQL")){
                dataset.write().option("saveMode",mode ).option("dbType",dbType)
                        .option("useSSL","false").option("numPartitions",numPartitions)
                        .jdbc(jdbcUrl, tableName, prop);
            }else if (dbType.equalsIgnoreCase("GREENPLUM") ||dbType.equalsIgnoreCase("POSTGRESQL")||dbType.equalsIgnoreCase("ORACLE")){
                dataset.write().option("saveMode",mode ).option("dbType",dbType).option("primaryKey",primaryKey)
                        .option("useSSL","false").option("numPartitions",numPartitions)
                        .jdbc(jdbcUrl, tableName, prop);
            }else{
                System.out.print("不支持" + dbType + "数据库的更新操作");
            }

        }else{
            dataset.write().mode(mode).option("useSSL", "false")
                    .jdbc(jdbcUrl, tableName, prop);
        }
    }
    public static void createGloabView(Dataset<Row> dataset,String viewName) {

        try {
            dataset.createGlobalTempView(viewName);
        } catch (AnalysisException e) {
            e.printStackTrace();
        }

        logger.info("完成全局视图创建:"+viewName);

    }

    public static SparkSession createSession(String databaseType) {
        SparkSession sparkSession = null;
        String type = databaseType.toLowerCase();
        switch (type) {
            case "hive":
                sparkSession = SparkSession
                        .builder()
                        .enableHiveSupport()
                        .getOrCreate();
                break;
            case "hive2":
                sparkSession = SparkSession
                        .builder()
                        .enableHiveSupport()
                        .getOrCreate();
                break;
            case "jdbc":
                sparkSession = SparkSession
                        .builder()
                        .getOrCreate();
                break;
            default:
                sparkSession = SparkSession
                        .builder()
                        .master("local[2]")
                        .getOrCreate();
                break;

        }

        return sparkSession;

    }

}
