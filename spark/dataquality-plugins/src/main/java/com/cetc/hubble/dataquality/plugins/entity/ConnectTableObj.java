package com.cetc.hubble.dataquality.plugins.entity;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

/**
 * 连接table需要的字段
 * @author Administrator
 *
 */
public class ConnectTableObj {

	/**
	 * SparkSession spark, String driver, String url, String tableName, String user, String password,Map<String,String> params, int numPartition
	 */
	private SparkSession sparkSession;
	private String driver;
	private String ip;
	private String port;
	private String dbName;
	private String tableName;
	private String userName;
	private String password;
	private Integer numPartition;
	private Integer lowerBound;
	private Integer upperBound;
	private String sqlStr;
	private String partitionColumn;
	
	public String getPartitionColumn() {
		return partitionColumn;
	}
	public void setPartitionColumn(String partitionColumn) {
		this.partitionColumn = partitionColumn;
	}
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getSqlStr() {
		return sqlStr;
	}
	public void setSqlStr(String sqlStr) {
		this.sqlStr = sqlStr;
	}
	public SparkSession getSparkSession() {
		return sparkSession;
	}
	public void setSparkSession(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}
	public String getDriver() {
		return driver;
	}
	public void setDriver(String driver) {
		this.driver = driver;
	}
	
	
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public Integer getNumPartition() {
		return numPartition;
	}
	public void setNumPartition(Integer numPartition) {
		this.numPartition = numPartition;
	}
	public Integer getLowerBound() {
		return lowerBound;
	}
	public void setLowerBound(Integer lowerBound) {
		this.lowerBound = lowerBound;
	}
	public Integer getUpperBound() {
		return upperBound;
	}
	public void setUpperBound(Integer upperBound) {
		this.upperBound = upperBound;
	}
}
