package com.cetc.hubble.dataquality.plugins.utils;

import com.cetc.hubble.dataquality.plugins.RegexPlugin;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
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


public class CommonFunc {
    private static Logger logger = LoggerFactory.getLogger(CommonFunc.class);

    public static void updateResult4OutDB(String outUrl, String outUsername
            , String outPassword, String outTable, String html, String sparkJobId,Float score) throws ClassNotFoundException, SQLException {
        String driverClass = getDriverClassByJDBCUrl(outUrl);
        String jdbcUrlWithUserPwd = outUrl + "?user=" + outUsername + "&password=" + outPassword;
        StringBuilder sb = new StringBuilder();
        sb.append(" update ")
                .append(outTable)
                .append(" set result = ")
                .append("'")
                .append(html)
                .append("'")
                .append(", score = ")
                .append(score)
                .append(" where spark_job_id = ")
                .append("'")
                .append(sparkJobId)
                .append("'");
        String updateSql = sb.toString();
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(driverClass);
            conn = DriverManager.getConnection(jdbcUrlWithUserPwd);
            logger.info("connected to " + jdbcUrlWithUserPwd);
            stmt = conn.createStatement();
            stmt.execute(updateSql);
        } finally {
            if (conn != null){
                conn.close();
            }
            if (stmt != null){
                stmt.close();
            }
        }
    }


    private static String getDriverClassByJDBCUrl(String outUrl) {
        if (outUrl.contains("jdbc:hive2://")) {
            return "org.apache.hive.jdbc.HiveDriver";
        } else if (outUrl.contains("jdbc:mysql://")) {
            return "com.mysql.jdbc.Driver";
        } else {
            throw new UnsupportedOperationException("不支持的数据库类型!!");
        }
    }

    public static String getResultUrl( Map<String, Long> result) {

        long totalCount  = result.get("totalCount");
        long unqualifiedCount = result.get("unqualifiedCount");
        long nullCount = result.get("nullCount");
        long qualifiedCount= result.get("qualifiedCount");

        String htmlResult =  " <html> " +
                "<div> " +
                "总数据条数: "+totalCount+"<n/> " +
                "合法数据条数: "+qualifiedCount+"<n/> " +
                "不合法数据条数: "+unqualifiedCount+"<n/> " +
                "空数据条数: "+nullCount+"<n/> " +
                "</div> </html>";
        return htmlResult;
    }



    public static String getIframUrl(String encodeIfram, String jobId) {

        String iframUrlDecode = null;


        try {
            iframUrlDecode = URLDecoder.decode(encodeIfram, "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.out.println("ifram decode 失败");
        }

        String ifram = iframUrlDecode.replace("$$$$", jobId);

        int startIndex = ifram.indexOf("{");
        String str1 = ifram.substring(0, startIndex);


        int lastIndex = ifram.lastIndexOf("}");
        String str2 = ifram.substring(startIndex, lastIndex + 1);

        int length = ifram.length();
        String str3 = ifram.substring(lastIndex + 1, length);

        System.out.println("ifram为：" + ifram);
        String iframdecode = null;
        try {
            iframdecode = URLEncoder.encode(str2, "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.out.println("ifram encode 失败");
        }

        String iframUrlEncode = str1 + iframdecode + str3;

        System.out.println(iframUrlEncode);

        return iframUrlEncode;
    }


    public static void writeMacroDataToMysqlV2(Map<String, Long> result, String outUrl, String macroTable, String outusername, String outpasswd, SparkSession sparkSession, String taskName) {

        Map<String, Long> resultdata = result;

        List structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("taskname", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("jobId", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("isvalid", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("checktype", DataTypes.StringType, false));
        structFields.add(DataTypes.createStructField("Count", DataTypes.LongType, false));
        structFields.add(DataTypes.createStructField("percent", DataTypes.DoubleType, false));
        structFields.add(DataTypes.createStructField("Checktime", DataTypes.TimestampType, false));

        Timestamp ts = new Timestamp(System.currentTimeMillis());

        StructType structType = DataTypes.createStructType(structFields);

        String jobId = sparkSession.sparkContext().applicationId();
        long totalCount = resultdata.get("totalCount");
        long unqualifiedCount = resultdata.get("unqualifiedCount");
        long qualifiedCount = resultdata.get("qualifiedCount");
        long nullCount = resultdata.get("nullCount");

        double unqualifiedPercentDouble = (unqualifiedCount * 10000 / totalCount) / (double) 100;
        double qualifiedPercentDouble = (qualifiedCount * 10000 / totalCount) / (double) 100;
        double nullPercentDouble = (nullCount * 10000 / totalCount) / (double) 100;


        Row rowData1 = RowFactory.create(taskName, jobId, "0", "不合格", unqualifiedCount, unqualifiedPercentDouble, ts);
        Row rowData2 = RowFactory.create(taskName, jobId, "1", "合格", qualifiedCount, qualifiedPercentDouble, ts);
        Row rowData3 = RowFactory.create(taskName, jobId, "0", "空", nullCount, nullPercentDouble, ts);


        List<Row> arrayList = new ArrayList<Row>();
        arrayList.add(rowData1);
        arrayList.add(rowData2);
        arrayList.add(rowData3);
        Dataset<Row> resultRow = sparkSession.sqlContext().createDataFrame(arrayList, structType);

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", outusername);
        connectionProperties.put("password", outpasswd);
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");
        resultRow.write().mode("append").jdbc(outUrl, macroTable, connectionProperties);

        logger.info("完成结果表数据写入");

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

    public static void writeDFToTable(Dataset dataset,String jdbcUrl, String tableName,String driver ,String outUserName, String outPassword,String mode,int numPartitions){

        Properties prop = new java.util.Properties();
        prop.setProperty("driver",driver );
        prop.setProperty("user", outUserName);
        prop.setProperty("password", outPassword);
        if(mode.equalsIgnoreCase("update")){
            dataset.write().option("saveMode", mode).option("useSSL","false").option("numPartitions",numPartitions)
                    .jdbc(jdbcUrl,tableName,prop);
        }else{
            dataset.write().mode(mode).option("useSSL", "false")
                    .jdbc(jdbcUrl, tableName, prop);
        }


    }
    /**
     * @param dataset
     * @param outDBType
     * @param outIp
     * @param outPort
     * @param outDbName
     * @param outTable
     * @param outUserName
     * @param outPassword
     *@param sourcedbType
     * @return
     */

    public  static void writeTable(Dataset dataset, String outDBType, String outIp, String outPort,
                                         String outDbName, String outTable, String outUserName, String outPassword, String sourcedbType,String mode) {
        writeTable(dataset, outDBType, outIp, outPort, outDbName, outTable, outUserName, outPassword, sourcedbType, mode, 1);
    }
    public static void writeTable(Dataset dataset, String outDBType, String outIp, String outPort,
                                         String outDbName, String outTable, String outUserName, String outPassword, String sourcedbType,String mode,Integer numPartitions) {
        if ("MYSQL".equalsIgnoreCase(outDBType)) {

            String jdbcUrl = new StringBuffer("jdbc:mysql://").append(outIp).append(":").append(outPort).append("/").append(outDbName).toString();
            System.out.println("Mysql链接信息如下： ");
            System.out.println("jdbcUrl : " + jdbcUrl);
            System.out.println("outUserName : " + outUserName);
            System.out.println("outPassword :" + outPassword);

            String driver = "com.mysql.jdbc.Driver";
            writeDFToTable(dataset, jdbcUrl, outTable, driver, outUserName, outPassword, mode, numPartitions);

        } else if ("ORACLE".equalsIgnoreCase(outDBType)) {

            String jdbcUrl;
            if (sourcedbType.equalsIgnoreCase("SID")) {
                jdbcUrl = new StringBuffer("jdbc:oracle:thin:@").append(outIp).append(":").append(outPort).append(":").append(outDbName).toString();
            } else {
                jdbcUrl = new StringBuffer("jdbc:oracle:thin:@").append(outIp).append(":").append(outPort).append("/").append(outDbName).toString();
            }
            System.out.println("ORACLE链接信息如下： ");
            System.out.println("jdbcUrl : " + jdbcUrl);
            System.out.println("outUserName : " + outUserName);
            System.out.println("outPassword :" + outPassword);
            String[] tableInfo = outTable.split("\\.");
            String tableTmp = tableInfo[1];
            String table = "\"" + tableTmp + "\"";
            String schemalTmp = tableInfo[0];
            String schemal = "\"" + schemalTmp + "\"";
            String schemalTable = schemal + "." + table;
            String driver = "oracle.jdbc.driver.OracleDriver";
            writeDFToTable(dataset, jdbcUrl, outTable, driver, outUserName, outPassword,mode,numPartitions);

        } else if ("GREENPLUM".equalsIgnoreCase(outDBType)) {

            String jdbcUrl = new StringBuffer("jdbc:postgresql://").append(outIp).append(":").append(outPort).append("/").append(outDbName).toString();
            System.out.println("GREENPLUM链接信息如下： ");
            System.out.println("jdbcUrl : " + jdbcUrl);
            System.out.println("outUserName : " + outUserName);
            System.out.println("outPassword :" + outPassword);
            String driver = "org.postgresql.Driver";
            writeDFToTable(dataset, jdbcUrl, outTable, driver, outUserName, outPassword,mode,numPartitions);

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
            writeDFToTable(dataset, jdbcUrl, outTable, driver, outUserName, outPassword,mode,numPartitions);
        }else {
            logger.info("没有匹配的类型");
        }

    }

    /**
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
    public static Dataset<Row> readTable(SparkSession spark, String inputDBType, String inputIp, String inputPort,
                                         String inputDbName, String inputTable, String inputUserName, String inputPassword, String sourcedbType) {


        return readTable(spark,inputDBType,inputIp,inputPort,inputDbName,inputTable,inputUserName,inputPassword,sourcedbType,null,null,false,null);

    }

    public static Dataset<Row> readTable(SparkSession spark, String inputDBType, String inputIp, String inputPort,
                                         String inputDbName, String inputTable, String inputUserName, String inputPassword, String sourcedbType,Map<String,String> params,Integer numPartition, Boolean isJX,String aggOP) {
        Dataset<Row> dataSet = null;
        if ("MYSQL".equalsIgnoreCase(inputDBType)) {

            String jdbcUrl = new StringBuffer("jdbc:mysql://").append(inputIp).append(":").append(inputPort).append("/").append(inputDbName).toString();
            System.out.println("Mysql链接信息如下： ");
            System.out.println("jdbcUrl : " + jdbcUrl);
            System.out.println("inputUserName : " + inputUserName);
            System.out.println("inputPassword :" + inputPassword);

            String driver = "com.mysql.jdbc.Driver";
            if(isJX == true){
                if(!StringUtils.isEmpty(aggOP) && (aggOP.equalsIgnoreCase("min") || aggOP.equalsIgnoreCase("max")) ){
                    dataSet= getDFAfterAggByJdbc(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword,params.get("partitionColumn"),aggOP);
                }else{
                    dataSet = getDFByJdbcWithJCSJC(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword,params,numPartition);
                }
            }else{
                dataSet = getDFByJdbc(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword);
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
            String driver = "oracle.jdbc.driver.OracleDriver";
            if(isJX == true){
                if(aggOP.equalsIgnoreCase("min") || aggOP.equalsIgnoreCase("max") ){
                    dataSet= getDFAfterAggByJdbc(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword,params.get("partitionColumn"),aggOP);
                }else{
                    dataSet = getDFByJdbcWithJCSJC(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword,params,numPartition);
                }
            }else{
                dataSet = getDFByJdbc(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword);
            }

        } else if ("GREENPLUM".equalsIgnoreCase(inputDBType)) {

            String jdbcUrl = new StringBuffer("jdbc:postgresql://").append(inputIp).append(":").append(inputPort).append("/").append(inputDbName).toString();
            System.out.println("GREENPLUM链接信息如下： ");
            System.out.println("jdbcUrl : " + jdbcUrl);
            System.out.println("inputUserName : " + inputUserName);
            System.out.println("inputPassword :" + inputPassword);
            String driver = "org.postgresql.Driver";
            if(isJX == true){
                if(aggOP.equalsIgnoreCase("min") || aggOP.equalsIgnoreCase("max") ){
                    dataSet= getDFAfterAggByJdbc(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword,params.get("partitionColumn"),aggOP);
                }else{
                    dataSet = getDFByJdbcWithJCSJC(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword,params,numPartition);
                }
            }else{
                dataSet = getDFByJdbc(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword);
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
            String driver = "dm.jdbc.driver.DmDriver";
            if(isJX == true){
                if(aggOP.equalsIgnoreCase("min") || aggOP.equalsIgnoreCase("max") ){
                    dataSet= getDFAfterAggByJdbc(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword,params.get("partitionColumn"),aggOP);
                }else{
                    dataSet = getDFByJdbcWithJCSJC(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword,params,numPartition);
                }
            }else{
                dataSet = getDFByJdbc(spark, driver, jdbcUrl, inputTable, inputUserName, inputPassword);
            }
        }else {
            logger.info("没有匹配的类型");
        }

        return dataSet;
    }



        /**
         * @param spark     SparkSession
         * @param url       读取表的url
         * @param tableName 表名
         * @param user      用户名
         * @param password  密码
         * @return
         */
    public static Dataset<Row> getDFByJdbc(SparkSession spark, String driver, String url, String tableName, String user, String password) {
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("driver", driver)
                .option("url", url)
                .option("dbtable", tableName)
                .option("user", user)
                .option("password", password)
                .option("fetchsize", "5000")
                .load();
        return jdbcDF;
    }
    /**
     * @param spark     SparkSession
     * @param url       读取表的url
     * @param tableName 表名
     * @param user      用户名
     * @param password  密码
     * @return
     */
    public static Dataset<Row> getDFByJdbcWithJCSJC(SparkSession spark, String driver, String url, String tableName, String user, String password,Map<String,String> params, int numPartition) {

        StringBuffer sb = new StringBuffer();
        sb.append("(select * from ")
                .append(tableName)
                .append(" where jcsjc = str_to_date('2100-01-01 00:00:00', '%Y-%m-%d %H:%i:%s') ")
                .append(") AS sourceTable");
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("useSSL", "false")
                .option("driver", driver)
                .option("url", url)
                .option("dbtable", sb.toString())
                .option("user", user)
                .option("password", password)
                .option("numPartitions", numPartition)
                .option("partitionColumn", params.get("partitionColumn"))
                .option("lowerBound", params.get("lowerBound")).option("upperBound", params.get("upperBound"))
                .option("fetchsize", "5000")
                .load();
        return jdbcDF;
    }
    public static Dataset<Row> getDFAfterAggByJdbc(SparkSession spark, String driver, String url, String tableName, String user, String password,String aggColumn,String aggOP) {

        StringBuffer sb = new StringBuffer();
        sb.append("(select ")
                .append(aggOP)
                .append("(")
                .append(aggColumn)
                .append(") from ")
                .append(tableName)
                .append(") AS sourceTable");
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("useSSL","false")
                .option("driver", driver)
                .option("url", url)
                .option("dbtable", sb.toString())
                .option("user", user)
                .option("password", password)
                .load();
        return jdbcDF;
    }

    /**
     * 创建全局视图
     *
     * @param dataset
     */
    public static void createGloabView(Dataset<Row> dataset) {

        createGloabView(dataset,null);

    }
    /**
     * 创建全局视图
     *
     * @param dataset
     */
    public static void createGloabView(Dataset<Row> dataset,String viewName) {
        try {
            if(!StringUtils.isEmpty(viewName)){
                dataset.createGlobalTempView(viewName);
                logger.info("完成全局视图创建:"+viewName);
            }else{
                dataset.createGlobalTempView("tmp_view");
                logger.info("完成全局视图创建:tmp_view");
            }
        } catch (AnalysisException e) {

        }
    }


    /**
     * 获取不合格而数据样例
     *
     * @param dataset
     * @param regxStr
     * @param cloum
     * @return
     */
    public static List<Row> getillegalData(Dataset<Row> dataset, final String regxStr, final String cloum) {


        List<Row> illegalData = null;

        JavaRDD<Row> javaRDD = dataset.toJavaRDD();

        JavaRDD resultData = javaRDD.filter(new Function<Row, Boolean>() {

            @Override
            public Boolean call(Row row) throws Exception {
                Integer index = row.fieldIndex(cloum);
                String cloumValue = String.valueOf(row.get(index));
                return !cloumValue.matches(regxStr);

            }
        });

        Long count = resultData.count();
        logger.info("不合格数据条数为 :" + count);

        if (count < 100) {
            illegalData = resultData.take(count.intValue());
        } else {
            illegalData = resultData.take(100);
        }

        logger.info("完成不合格数据采集");
        return illegalData;

    }


    /**
     * 不合格数据写入数据库
     * @param result
     * @param outUrl
     * @param detailTable
     * @param outUsername
     * @param outPassword
     * @param sparkSession
     */

    public static void writeIllgeaDataToMysql( List<Row> result,String outUrl,String detailTable,String outUsername,String outPassword,SparkSession sparkSession)
    {
        List<Row> resultData = result;
        List structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("jobId",DataTypes.StringType,false));
        structFields.add(DataTypes.createStructField("illgeadata",DataTypes.StringType,false));
        StructType structType = DataTypes.createStructType(structFields);

        Properties connectionProperties = new Properties();
        connectionProperties.put("user",outUsername);
        connectionProperties.put("password",outPassword);
        connectionProperties.put("driver","com.mysql.jdbc.Driver");

        Dataset<Row> resultRow = sparkSession.sqlContext().createDataFrame(resultData,structType);

        resultRow.write().mode("append").jdbc(outUrl,detailTable,connectionProperties);

        logger.info("完成异常数据详情写入");

    }

    /**
     * 讲数据转换为格式
     * @param illegalData
     * @param jobId
     * @param dataset
     * @return
     */

    public static  List<Row> handleIllegalData(List<Row> illegalData, String jobId, Dataset<Row> dataset) {


        List<Row> resultData = new ArrayList<Row>();

        String[] columnName = dataset.columns();
        for (Row row : illegalData) {
            JSONObject jsonMap = new JSONObject();
            for (int i = 0; i < columnName.length; i++) {
                String value = null;
                Boolean isNull = row.isNullAt(i);
                if (isNull) {
                    value = " ";
                } else {
                    value = row.get(i).toString().trim();
                }
                jsonMap.put(columnName[i], value);
            }
            String jsonStr = jsonMap.toString();
            Row rowData = RowFactory.create(jobId,jsonStr);
            resultData.add(rowData);

        }

        logger.info("完成不合格数据组装");

        return resultData;
    }

    /**
     * 保存非法数据标记表
     * @param sparkSession SparkSession
     * @param sourceDataset 源表数据集
     * @param sourceTableName 源表名
     * @param sourceTablePk 源表主键
     * @param checkColumn 被检测字段
     * @param regex 正则表达式
     * @param errorMessage 错误信息
     * @param checkStartTime 检测开始时间
     * @param markDbUrl 非法数据标记数据库URL
     * @param markTableName 非法数据标记表名
     * @param markTableUser 非法数据标记表用户名
     * @param markTablePassword 非法数据标记表密码
     * @return
     */
    public static void saveInvlidDataMarkTable(SparkSession sparkSession, Dataset<Row> sourceDataset, String sourceTableName,
                                   String sourceTablePk, String checkColumn, String regex, String errorMessage,
                                   java.util.Date checkStartTime, String markDbUrl,
                                   String markTableName, String markTableUser, String markTablePassword, String inputDbType) {

        String checkStartTimeFormatted = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(checkStartTime);

        StringBuffer logbuf = new StringBuffer("**************** saveInvlidDataMarkTable input params. sourceTableName: ").append(sourceTableName)
                .append(", sourceTablePk: ").append(sourceTablePk)
                .append(", checkColumn: ").append(checkColumn)
                .append(", regex: ").append(regex)
                .append(", errorMessage: ").append(errorMessage)
                .append(", checkStartTime: ").append(checkStartTimeFormatted)
                .append(", markDbUrl: ").append(markDbUrl)
                .append(", markTableName: ").append(markTableName)
                .append(", markTableUser: ").append(markTableUser)
                .append(", markTablePassword: ").append(markTablePassword)
                .append(" ****************");
        logger.info(logbuf.toString());

        StringBuffer filterSb = new StringBuffer(checkColumn).append(" not rlike '").append(regex).append("'");
        Dataset<Row> invalidDataPk = sourceDataset.filter(filterSb.toString()).select(sourceTablePk);
        List<Row> invalidDataPkList = invalidDataPk.collectAsList();

        List<Row> resultList = new ArrayList<>();
        Iterator<Row> it = invalidDataPkList.iterator();
        while (it.hasNext()) {
            Row errorId = it.next();
            Row newRow = RowFactory.create(
                    new Object[]{errorId.getAs(sourceTablePk).toString(),errorMessage,checkColumn,checkStartTimeFormatted
            });
            resultList.add(newRow);
        }

        StructType structType = new StructType(new StructField[]{
                new StructField("SOURCE_TABLE_PK", DataTypes.StringType, false, Metadata.empty()),
                new StructField("RESULT", DataTypes.StringType, true, Metadata.empty()),
                new StructField("COLUMN_NAME", DataTypes.StringType, true, Metadata.empty()),
                new StructField("CHECK_START_TIME", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> result = sparkSession.createDataFrame(resultList, structType);

        //System.out.println("******** result created ********");
        //result.show(false);

        String resultTable = getMarkTable(inputDbType, sourceTableName, markTableName);

        /*System.out.println("******** begin save mark table ********");
        StringBuffer loginfo = new StringBuffer("markDbUrl: ").append(markDbUrl)
                .append(", resultTable: ").append(resultTable)
                .append(", markTableUser: ").append(markTableUser)
                .append(", markTablePassword: ").append(markTablePassword);
        System.out.println(loginfo);*/

        result.write()
                .mode(SaveMode.Append)
                .format("jdbc")
                .option("driver", getDriverType(markDbUrl))
                .option("url",refineUrl(markDbUrl))
                .option("dbtable", resultTable)
                .option("user", markTableUser)
                .option("password", markTablePassword)
                .option("isolationLevel", "SERIALIZABLE")
                .save();

        logger.info("**************** finished save mark table ****************");
    }

    private static String refineUrl(String url) {

        if (url.contains("jdbc:mysql")) {
            StringBuffer refinedUrl = new StringBuffer(url);
            if (!url.contains("?")) {
                if (!url.contains("useUnicode=true") && !url.contains("characterEncoding=utf8")) {
                    refinedUrl.append("?useUnicode=true&characterEncoding=utf8");
                } else if (!url.contains("useUnicode=true")) {
                    refinedUrl.append("?useUnicode=true");
                } else if (!url.contains("characterEncoding=utf8")) {
                    refinedUrl.append("?characterEncoding=utf8");
                }
            } else {
                if (!url.contains("useUnicode=true") && !url.contains("characterEncoding=utf8")) {
                    refinedUrl.append("&useUnicode=true&characterEncoding=utf8");
                } else if (!url.contains("useUnicode=true")) {
                    refinedUrl.append("&useUnicode=true");
                } else if (!url.contains("characterEncoding=utf8")) {
                    refinedUrl.append("&characterEncoding=utf8");
                }
            }
            return refinedUrl.toString();
        }
        return url;
    }

    private static String getMarkTable(String inputDbType, String sourceTable, String markTable) {

        if (!StringUtils.isBlank(markTable)) {
            return markTable;
        }

        String sourceTableName;
        if ("ORACLE".equalsIgnoreCase(inputDbType)
                || "DAMENG".equalsIgnoreCase(inputDbType)) {
            String[] schemaAndTable = sourceTable.split("\\.");
            sourceTableName = schemaAndTable[schemaAndTable.length-1];
        } else {
            sourceTableName = sourceTable;
        }
        return "CHK_" + sourceTableName;
    }

    private static String getDriverType(String jdbcUrl) {
        if (jdbcUrl.contains("jdbc:oracle")) {
            return "oracle.jdbc.driver.OracleDriver";
        } else if (jdbcUrl.contains("jdbc:dm")) {
            return "dm.jdbc.driver.DmDriver";
        } else if (jdbcUrl.contains("jdbc:microsoft:sqlserver")) {
            return "com.microsoft.jdbc.sqlserver.SQLServerDriver";
        } else if (jdbcUrl.contains("jdbc:sqlserver")) {
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        } else if (jdbcUrl.contains("jdbc:postgresql")) {
            return "org.postgresql.Driver";
        } else {
            return "com.mysql.jdbc.Driver";
        }
    }

}
