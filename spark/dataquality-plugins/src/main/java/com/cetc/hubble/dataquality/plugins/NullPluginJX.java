package com.cetc.hubble.dataquality.plugins;

import com.cetc.hubble.dataquality.plugins.utils.CommonFunc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.HashMap;

public class NullPluginJX {
    private static Logger logger = LoggerFactory.getLogger(NullPluginJX.class);

    public static void main(String[] args) {
        //检测的数据库类型
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
        //检测字段
        String inputCheckColumn = "unit_name";

        //下面两个是有效的性能调优参数
        String mainKey = "id";
        int numPartition = 1;


        //source db is sid or servername
        String sourcedbType = "servername";

        //保存模式
        String saveMode = "update";




        SparkSession spark = CommonFunc.createSession(inputDBtype);

        HashMap<String ,String> paramsMap = new HashMap<String ,String>();
        paramsMap.put("partitionColumn",mainKey);

        //提供读取数据源的优化参数
        Dataset<Row> minValueDataset= CommonFunc.readTable(spark, inputDBtype, inputIp, inputPort, inputDbName, inputTable, inUserName, inPasswd, sourcedbType, paramsMap, null, true, "min");
        Integer lowerBound = minValueDataset.first().getAs(0);
        Dataset<Row> maxValueDataset= CommonFunc.readTable(spark, inputDBtype, inputIp, inputPort, inputDbName, inputTable, inUserName, inPasswd, sourcedbType, paramsMap, null, true, "max");
        Integer upperBound = maxValueDataset.first().getAs(0);

        paramsMap.put("lowerBound",String.valueOf(lowerBound));
        paramsMap.put("upperBound", String.valueOf(upperBound));



        Dataset<Row> preTotalDataset = CommonFunc.readTable(spark, inputDBtype, inputIp, inputPort, inputDbName, inputTable, inUserName, inPasswd, sourcedbType, paramsMap,numPartition,true,null);
        System.out.print("*****************"+inputTable+"的Schema信息******************");
        preTotalDataset.printSchema();
        Dataset<Row> totalDataset = preTotalDataset.drop("jcsjc");
        totalDataset.cache();
        CommonFunc.createGloabView(totalDataset);


        //获取jobId
        String sparkJobId = spark.sparkContext().applicationId();
        System.out.println("***************** sparkJobId： " + sparkJobId + " ******************");
        long totalCount = totalDataset.count();

        //获取检查合法的总条数
        StringBuilder legalSb = new StringBuilder();
        legalSb.append("select * from global_temp.tmp_view ")
                .append(" where ")
                .append(inputCheckColumn)
                .append(" is not null");
        Dataset<Row> legalDataset = spark.sql(legalSb.toString());

        Long legalNum = legalDataset.count();

        //不合法的总条数
        Long illegalNum = totalCount - legalNum;
        if (totalCount >0){
            BigDecimal legalNumDecimal = new BigDecimal(legalNum * 100);
            BigDecimal totalCountDecimal = new BigDecimal(totalCount);
            //分数取两位小数
            BigDecimal scoreDecimal = legalNumDecimal.divide(totalCountDecimal, 10, RoundingMode.CEILING).setScale(2, BigDecimal.ROUND_DOWN);
            float score = scoreDecimal.floatValue();

            logger.info("总条数为：" + totalCount);
            logger.info("合法条数为：" + legalNum);
            logger.info("不合法条数为：" + illegalNum);
            logger.info("此次检测的分数为 : " + score);
        }else{
            logger.info("没有需要检查的记录");
            return;
        }


        //获取不合法的dateset
        StringBuilder illegalSb = new StringBuilder();
        illegalSb.append("select * from global_temp.tmp_view ")
                .append(" where ")
                .append(inputCheckColumn)
                .append(" is null");

        Dataset<Row> illegalDataset = spark.sql(illegalSb.toString());
        //分别创建合法数据和不合法数据的全局视图
        CommonFunc.createGloabView(legalDataset,"legal_view");
        CommonFunc.createGloabView(illegalDataset, "illegal_view");

        //分别更新合法数据和不合法数据的jcsjc到源库
        String currentTime = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date());

        Dataset<Row> legalUpdateDS = spark.sql("select *, to_timestamp('" + currentTime + "','yyyy-MM-dd HH:mm:ss') AS jcsjc from global_temp.legal_view");
        Dataset<Row> illegalUpdateDS = spark.sql("select *, to_timestamp('2200-01-01 00:00:00','yyyy-MM-dd HH:mm:ss') AS jcsjc from global_temp.illegal_view");

       CommonFunc.writeTable(legalUpdateDS, inputDBtype, inputIp, inputPort, inputDbName, inputTable, inUserName, inPasswd, sourcedbType, saveMode,numPartition);
       CommonFunc.writeTable(illegalUpdateDS, inputDBtype, inputIp, inputPort, inputDbName, inputTable, inUserName, inPasswd, sourcedbType, saveMode,numPartition);

        //清理缓存
		totalDataset.uncache();
		//关闭会话和sparkContext
        spark.close();
        spark.sparkContext().stop();


    }
}
