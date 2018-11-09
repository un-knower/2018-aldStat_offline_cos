//==========================================================
/*gcs:
*终端分析，过去30天的数据，每天AM 7:00跑一次
*/

package aldwxstat

import java.sql.Timestamp

import aldwxconfig.ConfigurationUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import aldwxutils._

/**
  * Created by 18510 on 2017/10/30.
  */

object TerminalUserThirtyDays {
  def main(args: Array[String]): Unit = {
    /**
      * 对一个时间段的日志数据进行读取
      */
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "weilongsheng")
    val sparkseesion = SparkSession
      .builder()
      .appName(this.getClass.getName)
      //.config("spark.sql.shuffle.partitions", 24)
      .getOrCreate()

    //读取一星期的数据
    //val df = sparkseesion.read.parquet(data_files: _*).repartition(500)

//    val df = Aldstat_args_tool.analyze_args(args,sparkseesion,DBConf.hdfsUrl)
//    val df = ArgsTool.getLogs(args,sparkseesion,ConfigurationUtil.getProperty("tongji.parquet"))

    ArgsTool.analysisArgs(args)
    val du = ArgsTool.du
    val df = ArgsTool.getTencentSevenOrThirty(sparkseesion,du)
    val UpDataTime = new Timestamp(System.currentTimeMillis())

    val dateTime = TimeUtil.getTimestamp(TimeUtil.ytime())
    val terminal_sum = Array("wvv", "nt", "lang", "wv", "sv", "wsdk", "ww_wh")
    for (terminal <- terminal_sum) {
      var tmp_args = ""
      if (terminal == "ww_wh") {
        tmp_args = "concat(ww," + "'*'" + ",wh)"
      } else {
        tmp_args = terminal
      }
      df.createOrReplaceTempView(s"phone_brand_$terminal")
      //访问人数(visitor_count)
      val visnum = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} type_value,COUNT(DISTINCT uu) visitor_count FROM phone_brand_$terminal WHERE ev='app' GROUP BY ak,${tmp_args} ").distinct().na.fill(0).withColumn("type", lit(terminal))
      // visnum.show()

      visnum.foreachPartition((rows: Iterator[Row]) => {

        val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
        val sqlText = s"insert into aldstat_30days_terminal_analysis (app_key,day,type_value,visitor_count,type,update_at)" +
            s"values (?,?,?,?,?,?) ON " +
            s"DUPLICATE KEY UPDATE visitor_count=?,update_at=?"

        rows.foreach(r => {
          val day = dateTime
          val update_at = UpDataTime
          val app_key = r.get(0)
          val type_values = r.get(1)
          val visitor_count = r.get(2)
          val ty = r.get(3)

          params.+=(Array[Any](app_key,day,type_values,visitor_count,ty,update_at,visitor_count,update_at))
        })
        JdbcUtil.doBatch(sqlText,params)
      })
    }
    sparkseesion.close()
  }
}
