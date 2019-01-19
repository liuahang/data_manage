package com.cetc.hubble.dataquality.plugins;

import com.cetc.hubble.dataquality.plugins.utils.CommonFunc;
import com.cetc.hubble.dataquality.plugins.utils.FixedParamEnum;
import com.cetc.hubble.dataquality.plugins.utils.ParamsUtil;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

import static com.cetc.hubble.dataquality.plugins.utils.FixedParamEnum.*;

public class NullPlugin {
    private static Logger logger = LoggerFactory.getLogger(NullPlugin.class);

    public static void main(String[] args) {
        logger.info("接收的参数信息:{}", ParamsUtil.mkString(args,","));
        int fixedParamSize = FixedParamEnum.values().length;
        //如果args的个数少于固定参数个数 + 自定义参数个数,  则直接退出
        if(args.length < fixedParamSize + 1){
            logger.info("Usage: RegexPlugin <检查库类型>, <检查库IP>, <检查库端口>, <检查库库名>, <检查表名>, <检查库用户名>, <检查库密码>," +
                    "<回写库JDBC URL>, <回写表名>, <回写库用户名>, <回写库密码>, <检查字段> ");
            System.exit(1);
        }
        Map<FixedParamEnum, String> fixedParmas = ParamsUtil.parseFixed(args);
        //null.json中的自定义配置参数
        String checkColumn = ParamsUtil.eraseQuote(args[fixedParamSize + 0]);

        SparkSession spark = null;
        if ("HIVE2".equals(fixedParmas.get(INPUT_TYPE))){
            spark = SparkSession
                    .builder()
                    .enableHiveSupport()
                    .getOrCreate();
        }else {
            throw new UnsupportedOperationException("不支持的检测数据类型");
        }
        //获取jobId
        String sparkJobId = spark.sparkContext().applicationId();

        StringBuilder sb = new StringBuilder();
        sb.append("select count(*) from ")
                .append(fixedParmas.get(INPUT_DB_NAME))
                .append(".")
                .append(fixedParmas.get(INPUT_TABLE));
        String countSql = sb.toString();
        Long total =  spark.sql(countSql).first().getAs(0);
        sb.append(" where ")
                .append(checkColumn)
                .append(" is not null");
        String legalCountSql = sb.toString();
        Long legalNum  = spark.sql(legalCountSql).first().getAs(0);
        Long illegalNum = total - legalNum;
        String html =  " <html> " +
                "<div> " +
                "合法数据行数: "+legalNum+"<n/> " +
                "不合法数据行数: "+illegalNum+"<n/> " +
                "</div> </html>";
        try {
            CommonFunc.updateResult4OutDB(fixedParmas.get(OUT_URL), fixedParmas.get(OUT_USERNAME)
                    , fixedParmas.get(OUT_PASSWORD), fixedParmas.get(OUT_TABLE), html, sparkJobId,(float)0);
            logger.info("回写结果成功");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
