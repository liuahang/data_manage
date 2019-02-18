package com.cetc.hubble.dataquality.plugins.utils;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cetc.hubble.dataquality.plugins.entity.ConnectTableObj;
import com.cetc.hubble.dataquality.plugins.entity.WriteDataObj;

public class CommonUtils {
	private static Logger logger = LoggerFactory.getLogger(CommonUtils.class);
	private static String MAX = "MAX";
	private static String MIN = "MIN";

	/**
	  * 获取最大的rownumber
	 *@param connectObj 
	    * 需要的字段：tableName,driver,userName,password,ip,port,dbname,sparkSession
	 * @return 最大的rownumber值
	 */
	public static Double getMaxRowNumber(ConnectTableObj connectObj) {
		return getRowNumber(MAX, connectObj);
	}

	/**
	 * 获取最小的rownumber
	 * @param connectObj
	 * 需要的字段：tableName,driver,userName,password,ip,port,dbname,sparkSession
	 * @return 最小的rownumber值
	 */
	public static Double getMinRowNumber(ConnectTableObj connectObj) {
		return getRowNumber(MIN, connectObj);
	}

	 /**
	  * 获取质量检查前的数据集合
     * @param spark     SparkSession
     * @param url       读取表的url
     * @param tableName 表名
     * @param user      用户名
     * @param password  密码
     * @return
     */
    public static Dataset<Row> getBeforeCheckData(ConnectTableObj connectObj) {
    	String jdbcUrl = new StringBuffer("jdbc:mysql://").append(connectObj.getIp()).append(":").append(connectObj.getPort()).append("/")
				.append(connectObj.getDbName()).append("?useUnicode=true&characterEncoding=utf-8&useSSL=false").toString();
		 
        String sql = "(select * from (SELECT @rownum := @rownum +1 AS num,e.* FROM(SELECT @rownum := 0) r,"
        		+ connectObj.getTableName()
        		+ " e where jcsjc is null ) a) AS sourceTable";
        Dataset<Row> jdbcDF = connectObj.getSparkSession().read()
                .format("jdbc")
                .option("useSSL", "false")
                .option("driver", connectObj.getDriver())
                .option("url", jdbcUrl)
                .option("dbtable", sql)
                .option("user", connectObj.getUserName())
                .option("password", connectObj.getPassword())
                .option("numPartitions", connectObj.getNumPartition())
                .option("partitionColumn", connectObj.getPartitionColumn())
                .option("lowerBound", connectObj.getLowerBound()).option("upperBound", connectObj.getUpperBound())
                .option("fetchsize", "5000")
                .load();
        return jdbcDF;
    }
    
    
    public static void writeDFToTable(WriteDataObj writeObj){
    	String jdbcUrl = new StringBuffer("jdbc:mysql://").append(writeObj.getIp()).append(":").append(writeObj.getPort()).append("/")
				.append(writeObj.getDbName()).append("?useUnicode=true&characterEncoding=utf-8&useSSL=false").toString();
		 
        Properties prop = new java.util.Properties();
        prop.setProperty("driver",writeObj.getDriver() );
        prop.setProperty("user", writeObj.getUserName());
        prop.setProperty("password", writeObj.getPassword());
        if(writeObj.getMode().equalsIgnoreCase("update")){
        	writeObj.getDataset().write().option("saveMode", writeObj.getMode()).option("useSSL","false").option("numPartitions",writeObj.getNumPartition())
                    .jdbc(jdbcUrl,writeObj.getTableName(),prop);
        }else{
        	writeObj.getDataset().write().mode(writeObj.getMode()).option("useSSL", "false")
                    .jdbc(jdbcUrl, writeObj.getTableName(), prop);
        }
    }
	
	/**
	 * 创建mysql的sparkSession
	 * @return
	 */
	 public static SparkSession createSession() {
	        SparkSession sparkSession = null;
	        sparkSession = SparkSession
                    .builder()
                   //.master("local[2]")
                    .getOrCreate();
	        return sparkSession;

	    }
	
	 
	 
	private static Double getRowNumber(String type, ConnectTableObj connectObj) {
		String sqlStr = "";
		String jdbcUrl = new StringBuffer("jdbc:mysql://").append(connectObj.getIp()).append(":").append(connectObj.getPort()).append("/")
				.append(connectObj.getDbName()).append("?useUnicode=true&characterEncoding=utf-8&useSSL=false").toString();
		 
		logger.info("jdbcUrl=="+jdbcUrl);
		logger.info("password=="+connectObj.getPassword());
		if (type.equals(MAX)) {
			sqlStr = "(select max(rownum) from (SELECT @rownum := @rownum +1 AS rownum FROM(SELECT @rownum := 0) r,"
					+ connectObj.getTableName() 
					+ " e where jcsjc is null) a) AS sourcetable";
			//sqlStr = "(select max(oid) from "
			//		+ connectObj.getTableName() + "  where jcsjc is null) AS sourcetable";
			logger.info("max_sql==" + sqlStr);
		} else if (type.equals(MIN)) {
			sqlStr = "(select min(rownum) from (SELECT @rownum := @rownum +1 AS rownum FROM(SELECT @rownum := 0) r,"
					+ connectObj.getTableName() 
					+ " e where jcsjc is null) a) AS sourcetable";
			logger.info("min_sql==" + sqlStr);
		}
		Dataset<Row> jdbcDF = connectObj.getSparkSession().read().format("jdbc").option("useSSL", "false")
				.option("driver", connectObj.getDriver()).option("url", jdbcUrl).option("dbtable", sqlStr)
				.option("user", connectObj.getUserName()).option("password", connectObj.getPassword()).load();
		return jdbcDF.first().getAs(0);
	}
	
}
