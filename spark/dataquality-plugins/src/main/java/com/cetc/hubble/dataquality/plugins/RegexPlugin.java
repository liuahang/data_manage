package com.cetc.hubble.dataquality.plugins;

import com.cetc.hubble.dataquality.plugins.utils.AES;
import com.cetc.hubble.dataquality.plugins.utils.CommonFunc;
import com.cetc.hubble.dataquality.plugins.utils.ParamsUtil;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class RegexPlugin {
    static Logger logger = LoggerFactory.getLogger(RegexPlugin.class);

    public static void main(String[] args) {
        logger.info("接收的参数信息:{}", ParamsUtil.mkString(args, ","));

        String taskName = "数据正则检测";

        logger.info("接收的参数信息:{}", ParamsUtil.mkString(args, ","));
        //检测的数据库类型
        String inputDBtype = args[0];
        //ip
        String inputIp = args[1];
        //端口
        String inputPort = args[2];
        //数据库
        String inputDbName = args[3];
        //输入读取的表的名称
        String inputTable = args[4];
        //输入数据库用户名
        String inUserName = args[5];
        //输入数据库密码
        String inPasswdAES = args[6];
        //输出URL
        String outPutUrl = args[7];
        //输出表名
        String outputTable = args[8];
        //输出数据库用户名
        String outUserName = args[9];
        //输入数据库密码
        String outPasswd = args[10];
        //检测字段
        String inputCheckColumn = args[11];
        //正则表达式
        String regx = args[12];

        /** superset展示*/
        //superset地址
        String supersetUrl = args[13];
        //结果数据库地址
        String resultDbIp = args[14];
        //结果数据库端口
        String resultDbPort = args[15];
        //结果数据库名
        String resultDbName = args[16];
        //宏观数据库表
        String macroResultDbTableName = args[17];
        //异常数据详情表
        String detailResultDbTableName = args[18];
        //结果数据库用户名
        String resultDbUserName = args[19];
        //结果数据库用户密码
        String resultDbPasswd = args[20];

        //以下是输出非法数据标记表所需参数
        //源表主键
        String sourceTablePk= args[21];
        if (!valiateSourceTablePks(sourceTablePk)) {
            return;
        }
        //错误信息
        String errorMessage= args[22];
        //非法数据标记数据库URL
        String markDbUrl = args[23];
        //非法数据标记表名
        String markTableName = args[24];
        //非法数据标记表用户名
        String markTableUser = args[25];
        //非法数据标记表密码
        String markTablePassword = args[26];
        //source db is sid or servername
        String sourcedbType = args[27];

        java.util.Date checkStartTime = new java.util.Date();

        String inPasswd = AES.Decrypt(inPasswdAES, "HubbleStarMetaGT");

        SparkSession spark = null;

        spark = CommonFunc.createSession(inputDBtype);

        Dataset<Row> dataset = CommonFunc.readTable(spark, inputDBtype, inputIp, inputPort, inputDbName, inputTable, inUserName, inPasswd, sourcedbType);

        CommonFunc.createGloabView(dataset);


        //获取jobId
        String sparkJobId = spark.sparkContext().applicationId();
        System.out.println("***************** sparkJobId： " + sparkJobId + " ******************");
        long totalCount = dataset.count();

        //获取检查合法的总条数
        StringBuilder sb = new StringBuilder();

        sb.append("select count(*) from global_temp.tmp_view");
        String checkColumn = null;
        String javaRegex = null;
        sb.append(" where ");
        checkColumn = ParamsUtil.eraseQuote(inputCheckColumn);
        //spark sql 需要对 \ 转义， 比如正则 \d  需要转义为 \\d
        javaRegex = ParamsUtil.eraseQuote(regx).replace("\\", "\\\\");
        sb.append("cast(")
                .append(checkColumn)
                .append(" as string)")
                .append(" regexp ")
                .append("'")
                .append(javaRegex)
                .append("'");

        String legalCountSql = sb.toString();
        logger.info("检测sql为：" + legalCountSql);

        String sqlnullcount = "SELECT count(*) from global_temp.tmp_view WHERE " + checkColumn + " regexp '^$' or " + checkColumn + " is null";
        Dataset<Row> datasetNull = spark.sql(sqlnullcount);

        Long nullNum = datasetNull.first().getAs(0);

        Long legalNum = spark.sql(legalCountSql).first().getAs(0);
        long illegalNum = totalCount - legalNum - nullNum;
        BigDecimal legalNumDecimal =  new  BigDecimal(legalNum * 100);
        BigDecimal  totalCountDecimal  =  new  BigDecimal(totalCount);
        //分数取两位小数
        BigDecimal scoreDecimal = legalNumDecimal.divide(totalCountDecimal,10, RoundingMode.CEILING).setScale(2, BigDecimal.ROUND_DOWN);
        float score = scoreDecimal.floatValue();

        logger.info("总条数为：" + totalCount);
        logger.info("合法条数为：" + legalNum);
        logger.info("不合法条数为：" + illegalNum);
        logger.info("空值数据条数为 : " + nullNum);
        logger.info("此次检测的分数为 : " + score);


        Map<String, Long> result = new HashedMap();
        result.put("totalCount", totalCount);
        result.put("unqualifiedCount", illegalNum);
        result.put("nullCount", nullNum);
        result.put("qualifiedCount", legalNum);

        //save invalid data mark table
        CommonFunc.saveInvlidDataMarkTable(spark, dataset, inputTable, sourceTablePk, inputCheckColumn,
                regx, errorMessage, checkStartTime, markDbUrl, markTableName, markTableUser, markTablePassword, inputDBtype);

        //如果没有配置superset，则直接在程序中构建html
        String html=null;
        if (StringUtils.isBlank(supersetUrl)) {
            html = CommonFunc.getResultUrl(result);
            //如果配置superset，则用superset构建html
        } else {
            String resultDbUrl = "jdbc:mysql://" + resultDbIp + ":" + resultDbPort + "/" + resultDbName + "?useUnicode=true&characterEncoding=utf-8";
            //统计数据写入
            CommonFunc.writeMacroDataToMysqlV2(result, resultDbUrl, macroResultDbTableName, resultDbUserName, resultDbPasswd, spark, taskName);
            /*******详情数据写入********/
            List<Row> illegalDataSample = CommonFunc.getillegalData(dataset, ParamsUtil.eraseQuote(regx), checkColumn);
            List<Row> illegalData = CommonFunc.handleIllegalData(illegalDataSample, sparkJobId, dataset);
            CommonFunc.writeIllgeaDataToMysql(illegalData, resultDbUrl, detailResultDbTableName, resultDbUserName, resultDbPasswd, spark);
            html = CommonFunc.getIframUrl(supersetUrl, sparkJobId);
        }

        try {
            CommonFunc.updateResult4OutDB(outPutUrl, outUserName, outPasswd, outputTable, html, sparkJobId, score);
            logger.info("ifram 数据库写入成功");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        logger.info("正则检测程序结束");

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        spark.catalog().dropGlobalTempView("tmp_view");
        spark.stop();
        spark.close();

    }

    private static boolean valiateSourceTablePks(String sourceTablePk) {
        if (StringUtils.isBlank(sourceTablePk)) {
            logger.error("源表主键为空");
            return false;
        }
        return true;
    }
}
