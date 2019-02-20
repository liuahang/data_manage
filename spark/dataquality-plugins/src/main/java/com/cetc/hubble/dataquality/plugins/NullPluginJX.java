package com.cetc.hubble.dataquality.plugins;

import com.cetc.hubble.dataquality.plugins.utils.CommonFunc;
import com.cetc.hubble.dataquality.plugins.utils.CommonFuncJX;
import com.cetc.hubble.dataquality.plugins.utils.FixedParamEnum;
import com.cetc.hubble.dataquality.plugins.utils.ParamsUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class NullPluginJX {
    private static Logger logger = LoggerFactory.getLogger(NullPluginJX.class);

    public static void main(String[] args) {
        //检测的数据库类型
        String inputDBtype = "postgresql";
        //ip
        String inputIp = "192.168.3.153";
        //端口
        String inputPort = "5432";
        //数据库
        String inputDbName = "test";
        //输入读取的表的名称
        String inputTable = "wh_re_units";
        //输入数据库用户名
        String inUserName = "postgres";
        //输入数据库密码
        String inPasswd = "postgres";
        //检测字段
        String CheckColumn = "unit_name";

        //source db is sid or servername
        String sourcedbType = "servername";

        String partitionColumn = "rownum";

        String numPartitions ="20";

        String saveMode = "update";

        String primaryKey = "id";


        SparkSession spark = CommonFuncJX.createSession(inputDBtype);
        //固定数据库连接参数
        String[] fixArgs = new String[]{inputDBtype,inputIp,inputPort,inputDbName,inputTable,inUserName,inPasswd};
        Map<FixedParamEnum, String> fixArgsMap = ParamsUtil.parseFixedJX(fixArgs);

        //JDBC读写时的配置
        Map<String,String> JDBCProperties = new HashMap<String,String>();
        //读数据时，numPartition配合lowerBound,upperBound一起使用
        JDBCProperties.put("numPartitions",numPartitions);
        JDBCProperties.put("partitionColumn",partitionColumn);
        //数据写入时需要用到的参数
        JDBCProperties.put("saveMode",saveMode);
        JDBCProperties.put("primaryKey",primaryKey);


        //提供读取数据源的优化参数
        /*Dataset<Row> minValueDataset= CommonFunc.readTable(spark, inputDBtype, inputIp, inputPort, inputDbName, inputTable, inUserName, inPasswd, sourcedbType, paramsMap, null, true, "min");
        Integer lowerBound = minValueDataset.first().getAs(0);
        Dataset<Row> maxValueDataset= CommonFunc.readTable(spark, inputDBtype, inputIp, inputPort, inputDbName, inputTable, inUserName, inPasswd, sourcedbType, paramsMap, null, true, "max");
        Integer upperBound = maxValueDataset.first().getAs(0);*/

        int lowerBound = 1;
        Dataset<Row> maxValueDataset= CommonFuncJX.readTable(spark, fixArgsMap, JDBCProperties, "count",sourcedbType);
        long upperBound = maxValueDataset.first().getAs(0);


        JDBCProperties.put("lowerBound", String.valueOf(lowerBound));
        JDBCProperties.put("upperBound", String.valueOf(upperBound));

        Dataset<Row> preTotalDataset = CommonFuncJX.readTable(spark, fixArgsMap, JDBCProperties,null,sourcedbType);

        System.out.print("*****************" + inputTable + "的Schema信息******************");
        preTotalDataset.printSchema();
        Dataset<Row> totalDataset = preTotalDataset.drop("rownum").drop("jcsjc");

        //总数据的全局视图
        CommonFuncJX.createGloabView(totalDataset,"total_view");


        //获取jobId
        String sparkJobId = spark.sparkContext().applicationId();
        System.out.println("***************** sparkJobId： " + sparkJobId + " ******************");
        long totalCount = upperBound;

        //获取合法的Dataset
        StringBuilder legalSb = new StringBuilder();
        legalSb.append("select * from global_temp.total_view ")
                .append(" where ")
                .append(CheckColumn)
                .append(" is not null");
        Dataset<Row> legalDataset = spark.sql(legalSb.toString());

        //获取不合法的dateset
        StringBuilder illegalSb = new StringBuilder();
        illegalSb.append("select * from global_temp.total_view ")
                .append(" where ")
                .append(CheckColumn)
                .append(" is null");

        Dataset<Row> illegalDataset = spark.sql(illegalSb.toString());
        //分别创建合法数据和不合法数据的全局视图
        CommonFuncJX.createGloabView(legalDataset,"legal_view");
        CommonFuncJX.createGloabView(illegalDataset, "illegal_view");

        //分别更新合法数据和不合法数据的jcsjc到源库
        String currentTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        Dataset<Row> legalUpdateDS = spark.sql("select *, to_timestamp('" + currentTime + "','yyyy-MM-dd HH:mm:ss') AS jcsjc from global_temp.legal_view");

        Dataset<Row> illegalUpdateDS = spark.sql("select *, to_timestamp('2200-01-01 00:00:00','yyyy-MM-dd HH:mm:ss') AS jcsjc from global_temp.illegal_view");
        legalUpdateDS.cache();

       CommonFuncJX.writeTable(legalUpdateDS, inputDBtype, inputIp, inputPort, inputDbName, inputTable, inUserName, inPasswd, sourcedbType,JDBCProperties);
        CommonFuncJX.writeTable(illegalUpdateDS, inputDBtype, inputIp, inputPort, inputDbName, inputTable, inUserName, inPasswd, sourcedbType, JDBCProperties);

        /**
         * 以下是统计信息
         */
        //获取检查合法的总条数
        Long legalNum = legalUpdateDS.count();
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

        //关闭会话和sparkContext
        spark.close();
        spark.sparkContext().stop();


    }
}
