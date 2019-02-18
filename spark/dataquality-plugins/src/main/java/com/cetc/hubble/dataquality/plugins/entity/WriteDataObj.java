package com.cetc.hubble.dataquality.plugins.entity;

import org.apache.spark.sql.Dataset;

/**
 * 写入数据到表需要的属性对象
 * @author Administrator
 *
 */
public class WriteDataObj {

	//Dataset dataset,String jdbcUrl, String tableName,String driver ,String outUserName, String outPassword,String mode,int numPartitions
	private Dataset dataset ;
	private String mode;
	private String driver;
	private String ip;
	private String port;
	private String dbName;
	private String tableName;
	private String userName;
	private String password;
	private Integer numPartition;
	
	
	public Dataset getDataset() {
		return dataset;
	}
	public void setDataset(Dataset dataset) {
		this.dataset = dataset;
	}
	public String getMode() {
		return mode;
	}
	public void setMode(String mode) {
		this.mode = mode;
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
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
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
	
	
	
}
