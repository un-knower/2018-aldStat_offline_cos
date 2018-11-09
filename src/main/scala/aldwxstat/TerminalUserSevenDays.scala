//==========================================================
/*gcs:
*终端分析，过去七天7天的数据，每天AM 6:45 跑一次
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
object TerminalUserSevenDays {
  def main(args: Array[String]): Unit = {
    /**
      * 对一个时间段的日志数据进行读取
      */
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "weilongsheng")


    val sparkseesion = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .getOrCreate()

    //读取一星期的数据

//    val df = Aldstat_args_tool.analyze_args(args,sparkseesion,DBConf.hdfsUrl)

//    val df = ArgsTool.getLogs(args,sparkseesion,ConfigurationUtil.getProperty("tongji.parquet"))
    ArgsTool.analysisArgs(args) //gcs:将args 当中的-d -du 等信息都解析出来

    val du = ArgsTool.du //gcs:取出ArgsTool的du。du当中存储的是读取多少天的数据

    val df = ArgsTool.getTencentSevenOrThirty(sparkseesion,du)
    val UpDataTime = new Timestamp(System.currentTimeMillis()) //gcs:获得当前的时间

    val dateTime = TimeUtil.getTimestamp(TimeUtil.ytime()) //gcs:把string类型的时间转换成Timestamp类型的数据

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
      //gcs:进行筛选之后，将没有数据的类用0代替，之后再添加一列
      // visnum.show()

      visnum.foreachPartition((rows: Iterator[Row]) => {

        val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
        val sqlText = s"insert into aldstat_7days_terminal_analysis (app_kday,type_value,visitor_count,type,update_at)" +
            s"values (?,?,?,?,?,?) ON " +
            s"DUPLICATE KEY UPDATE visitor_count=?,update_at=?" //gcs:如果发现了重复的数据就会更新数据，但是不会从新插入。，

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
