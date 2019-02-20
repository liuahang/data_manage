package com.cetc.hubble.dataquality.plugins;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cetc.hubble.dataquality.plugins.entity.ConnectTableObj;
import com.cetc.hubble.dataquality.plugins.entity.WriteDataObj;
import com.cetc.hubble.dataquality.plugins.utils.AES;
import com.cetc.hubble.dataquality.plugins.utils.CommonFunc;
import com.cetc.hubble.dataquality.plugins.utils.CommonUtils;
import com.cetc.hubble.dataquality.plugins.utils.ParamsUtil;

/**
 * 质量检查主类
 * 
 * @author Administrator
 *
 */
public class QualityCheck {
	private static Logger logger = LoggerFactory.getLogger(QualityCheck.class);

	public static void main(String[] args) {
		logger.info("接收的参数信息:{}", ParamsUtil.mkString(args, ","));
		String taskName = "数据质量检查测试";
		// 检测的数据库类型
		String inputDBtype = args[0];
		// ip
		String inputIp = args[1];
		// 端口
		String inputPort = args[2];
		// 数据库
		String inputDbName = args[3];
		// 输入读取的表的名称
		String inputTable = args[4];
		// 输入数据库用户名
		String inUserName = args[5];
		// 输入数据库密码
		String inPasswdAES = args[6];
		// 输出URL
		String outPutUrl = args[7];
		// 输出表名
		String outputTable = args[8];
		// 输出数据库用户名
		String outUserName = args[9];
		// 输出数据库密码
		String outPasswd = args[10];
		// 检查字段
		String seq1 = args[11];
		String seq2 = args[12];
		/*//检测的数据库类型
		String inputDBtype = "mysql";
		//ip
		String inputIp = "192.168.3.154";
		//端口
		String inputPort = "3306";
		//数据库
		String inputDbName = "spark_etl_test";
		//输入读取的表的名称
		String inputTable = "wh_re_units";
		//输入数据库用户名
		String inUserName = "root";
		//输入数据库密码
		String inPasswd = "Lxx123456";
		String inPasswdAES = "Lxx123456";
		// 输出URL
		String outPutUrl = "";
		// 输出表名
		String outputTable =  "";
		// 输出数据库用户名
		String outUserName =  "";
		// 输出数据库密码
		String outPasswd =  "";
		// 检查字段
		String seq1 = "unit_name";
		String seq2 = "unit_code";*/

		printLogInfo("inputIp", inputIp);
		printLogInfo("inputPort", inputPort);
		printLogInfo("inputDbName", inputDbName);
		printLogInfo("inputTable", inputTable);
		printLogInfo("inUserName", inUserName);
		printLogInfo("inPasswdAES", inPasswdAES);
		printLogInfo("outPutUrl", outPutUrl);
		printLogInfo("outputTable", outputTable);
		printLogInfo("outUserName", outUserName);
		printLogInfo("outPasswd", outPasswd);
		printLogInfo("seq1", seq1);
		printLogInfo("seq2", seq2);

		String driver = "com.mysql.jdbc.Driver";
		// String driver = "com.mysql.cj.jdbc.Driver";
		// 密码解密
		String inPasswd = AES.Decrypt(inPasswdAES, "HubbleStarMetaGT");
		ConnectTableObj connectObj = new ConnectTableObj();
		connectObj.setDriver(driver);
		connectObj.setUserName(inUserName);
		connectObj.setPassword(inPasswd);
		connectObj.setTableName(inputTable);
		connectObj.setDbName(inputDbName);
		connectObj.setIp(inputIp);
		connectObj.setPort(inputPort);
		SparkSession spark = CommonUtils.createSession();
		connectObj.setSparkSession(spark);
		Double maxRowNumber = CommonUtils.getMaxRowNumber(connectObj);
		Double minRowNumber = CommonUtils.getMinRowNumber(connectObj);
		Integer maxRowNumberInt = Integer.parseInt(new java.text.DecimalFormat("0").format(maxRowNumber));
		Integer minRowNumberInt = Integer.parseInt(new java.text.DecimalFormat("0").format(minRowNumber));
		printLogInfo("maxRowNumber", maxRowNumberInt.toString());
		printLogInfo("minRowNumber", minRowNumberInt.toString());
		connectObj.setUpperBound(maxRowNumberInt);
		connectObj.setLowerBound(minRowNumberInt);
		connectObj.setPartitionColumn("num");
		connectObj.setNumPartition(4);
		Dataset<Row> preTotalDataset = CommonUtils.getBeforeCheckData(connectObj);
		System.out.print("*****************" + inputTable + "的Schema信息******************");
		preTotalDataset.printSchema();
		Dataset<Row> totalDataset = preTotalDataset.drop("jcsjc").drop("num");
		totalDataset.cache();

		CommonFunc.createGloabView(totalDataset);

		// 获取jobId
		String sparkJobId = spark.sparkContext().applicationId();
		System.out.println("***************** sparkJobId： " + sparkJobId + " ******************");
		long totalCount = totalDataset.count();

		// 获取检查合法的总条数
		StringBuilder legalSb = new StringBuilder();
		legalSb.append("select * from global_temp.tmp_view ").append(" where ").append(seq1)
				.append(" !='' ");
		Dataset<Row> legalDataset = spark.sql(legalSb.toString());

		Long legalNum = legalDataset.count();
		logger.info("legalNum==" + legalNum);
		// 不合法的总条数
		Long illegalNum = totalCount - legalNum;
		logger.info("illegalNum==" + illegalNum);
		if (totalCount > 0) {
			BigDecimal legalNumDecimal = new BigDecimal(legalNum * 100);
			BigDecimal totalCountDecimal = new BigDecimal(totalCount);
			// 分数取两位小数
			BigDecimal scoreDecimal = legalNumDecimal.divide(totalCountDecimal, 10, RoundingMode.CEILING).setScale(2,
					BigDecimal.ROUND_DOWN);
			float score = scoreDecimal.floatValue();

			logger.info("总条数为：" + totalCount);
			logger.info("合法条数为：" + legalNum);
			logger.info("不合法条数为：" + illegalNum);
			logger.info("此次检测的分数为 : " + score);
		} else {
			logger.info("没有需要检查的记录");
			return;
		}

		// 获取不合法的dateset
		StringBuilder illegalSb = new StringBuilder();
		illegalSb.append("select * from global_temp.tmp_view ").append(" where ").append(seq1)
				.append(" ='' ");

		Dataset<Row> illegalDataset = spark.sql(illegalSb.toString());
		// 分别创建合法数据和不合法数据的全局视图
		CommonFunc.createGloabView(legalDataset, "legal_view");
		CommonFunc.createGloabView(illegalDataset, "illegal_view");

		// 分别更新合法数据和不合法数据的jcsjc到源库
		String currentTime = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date());

		Dataset<Row> legalUpdateDS = spark.sql("select *, now() AS jcsjc from global_temp.legal_view");
		//Dataset<Row> illegalUpdateDS = spark.sql(
		//		"select *, to_timestamp('2200-01-01 00:00:00','yyyy-MM-dd HH:mm:ss') AS jcsjc from global_temp.illegal_view");

		WriteDataObj wobj = new WriteDataObj();
		wobj.setDriver(driver);
		wobj.setUserName(inUserName);
		wobj.setPassword(inPasswd);
		wobj.setTableName(inputTable);
		wobj.setDbName(inputDbName);
		wobj.setIp(inputIp);
		wobj.setPort(inputPort);
		wobj.setDataset(legalUpdateDS);
		wobj.setNumPartition(4);
		wobj.setMode("update");
		CommonUtils.writeDFToTable(wobj);

		// 关闭会话和sparkContext
		spark.close();
		spark.sparkContext().stop();

	}

	private static void printLogInfo(String name, String value) {
		logger.info(name + "==" + value);
	}
}
