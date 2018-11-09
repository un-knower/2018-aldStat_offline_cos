package aldwxstat

import aldwxconfig.ConfigurationUtil
import aldwxutils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * 受访页的分享次数
  */

object PageViewShare {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "zhangyanpeng")
    //获取昨天的日期
    val yesterday = TimeUtil.processArgs(args)
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .config("spark.sql.shuffle.partitions", 12)
      .getOrCreate()


    //    val df = Aldstat_args_tool.analyze_args(args,sparkSession,DBConf.hdfsUrl).filter("ak!='null' and ev='event'").cache()
    val df = ArgsTool.getLogs(args, sparkSession, ConfigurationUtil.getProperty("tongji.parquet")).filter("ak!='null' and ev='event'").cache()

    df.createTempView("page")
    val rs1 = sparkSession.sql("SELECT ak,path,COUNT(path) share_count FROM page WHERE ev='event' and ct!= 'fail' and ct !='null' and tp='ald_share_status' GROUP BY ak,path")
    val rs2 = sparkSession.sql("SELECT ak,COUNT(path) share_count FROM page WHERE ev='event' and ct!= 'fail' and ct !='null' and tp='ald_share_status' GROUP BY ak")

    //    rs1.show()
    //    rs2.show()
    rs1.foreachPartition((rows: Iterator[Row]) => {
      //      val conn =MySqlPool.getJdbcConn()
      //      statement = conn.createStatement()
      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement()
      try {
        //        conn.setAutoCommit(false)
        rows.foreach(r => {
          val app_key = r(0)
          val day = yesterday
          val page_path = r(1)
          val share_count = r(2) //分享次数
          val update_at = TimeUtil.nowInt()

          val sql = s"update aldstat_page_view  set share_count='${share_count}' where day='${day}' and app_key='${app_key}' and page_path='${page_path}'"
          statement.addBatch(sql)
        })
        statement.executeBatch
        conn.commit()
      } catch {
        case e: Exception => e.printStackTrace()
          conn.close()
      }
    })

    rs2.foreachPartition((rows: Iterator[Row]) => {
      val params=new ArrayBuffer[Array[Any]]()
      val sql = s"update aldstat_daily_page_view set share_count=? where day=? and app_key=?"
      rows.foreach(r => {
        val app_key = r(0)
        val day = yesterday
        val share_count = r(1) //分享次数
        val update_at = TimeUtil.nowInt()
        params.+=(Array(share_count,day,app_key))
      })
      JdbcUtil.doBatch(sql,params)
    })

    sparkSession.stop()
  }
}
