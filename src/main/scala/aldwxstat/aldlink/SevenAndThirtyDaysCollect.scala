package aldwxstat.aldlink

import java.sql.Timestamp

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangtaiyang on 2017/12/14. 
  */
class SevenAndThirtyDaysCollect {
  def executeSevenOrDaysCollect(spark: SparkSession, numDays: String): Unit = {
//    val logs = ArgsTool.getSevenOrThirtyDF(spark, ConfigurationUtil.getProperty("tongji.parquet"), numDays)
    val logs=ArgsTool.getTencentDailyDataFrame(spark, ConfigurationUtil.getProperty("tencent.parquet"))
    println(s"开始处理${numDays}天汇总的数据")

    spark.sql("select ak,uu from visitora_page").createOrReplaceTempView("visitora_page")

    val day = ArgsTool.day
    println("查询访问人数并插入到mysql")
    //先将查到的访问人数插入到mysql
    spark.sql("SELECT ak app_key,COUNT(DISTINCT uu) visitor_count FROM visitora_page GROUP BY ak")
      .na.fill("0").na.fill(0).foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      //      val sqlText =
      //        s"""
      //          |replace into aldstat_${numDays}days_link_summary(app_key,day,link_visitor_count)values(?,?,?)
      //        """.stripMargin
      val sqlText =
      s"""
         |insert into aldstat_${numDays}days_link_summary(app_key,day,link_visitor_count)values(?,?,?)
         |ON DUPLICATE KEY UPDATE link_visitor_count=?
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val appKey = row.get(0).toString
        val visitorCount = row.get(1).toString
        println("打印7天或30天中的day=" + day)

        params.+=(Array[Any](appKey, day, visitorCount,visitorCount))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库
    })
  }
}
