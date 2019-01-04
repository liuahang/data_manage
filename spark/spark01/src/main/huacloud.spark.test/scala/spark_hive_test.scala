import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
 * Created by Wping on 2018/9/17.
 */
object spark_hive_test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
	  .config(new SparkConf())
      //.master("local[4]")
      .appName("SparkSQL-spark_hive_test")
      .enableHiveSupport()
      .getOrCreate()
    val df: DataFrame = spark.sql("show databases")
    df.show()

  }
}
