//==========================================================
/*gcs:
*转化漏斗
*/


import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer



/**
  * Created by gaoxiang on 2018/1/11.
  */
object EventDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)  //gcs:设置log日志的级别


    val ss = SparkSession.builder()
      .appName(this.getClass.getName)
      .getOrCreate()


    //获取昨天时间
    val yesterday = aldwxutils.TimeUtil.processArgs(args)

    //==========================================================1
    //gcs:主程序
    allEventStatistics(ss, yesterday, args)

    ss.close()
  }


  /**
    * <b>author:</b> gcs <br>
    * <b>data:</b> 18-5-8 <br>
    * <b>description:</b><br>
    *   spark: SparkSession ; <br>
    *    yesterday: String ; yesterday 这个时间的作用是什么呢<br>
    * <b>param:</b><br>
    * <b>return:</b><br>
    */
  def allEventStatistics(spark: SparkSession, yesterday: String, args: Array[String]) = {


    ArgsTool.analysisArgs(args)

    ArgsTool.day = "2018-05-09"
    ArgsTool.ak = "d3163f1932830f8da11368d27d32f9a4"

    val du = ArgsTool.du

    val df = ArgsTool.getTencentSevenOrThirty(spark,du) //gcs:按照du，读取du天数的数据

    df.createTempView("daily_event") //gcs:创建一个临时的视图

    val result = spark.sql("select * from daily_event where tp='rw_finish'")

    result.show()

  }

}
