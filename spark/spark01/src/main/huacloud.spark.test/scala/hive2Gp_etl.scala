import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by Wping on 2018/12/29.
 */
object hive2Gp_etl {
  val config = ConfigFactory.load()
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
	  .config(new SparkConf())
      //.master("local[4]")
      .appName("hive2Gp_gj_gps_history")
      .enableHiveSupport()
      .getOrCreate()
    val df = spark.sql("select * from jxsjtj.hive_gj_gps_history")
    WriteGpProcess(df)
  }
  def WriteGpProcess(df :DataFrame): Unit ={
    val prop = new java.util.Properties
    prop.setProperty("driver", config.getString(Constants.GP_DB_DRIVER))
    prop.setProperty("user", config.getString(Constants.GP_DB_USRNAME))
    prop.setProperty("password", config.getString(Constants.GP_DB_PASSWORD))

    val url = config.getString(config.getString(Constants.GP_DB_URL))
    df.write.mode("append").jdbc(url,"gj_gps_history",prop)
  }
}
