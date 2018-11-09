package aldwxstat

import java.sql.Statement

import aldwxconfig.ConfigurationUtil
import aldwxutils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangyanpeng on 2017/8/4.
  *
  *
  * 7日 事件统计
  */
@deprecated("Use EventParas instead.", "wangtaiyang")
object EventThirtyDays {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.ERROR)
    AldwxDebug.debug_info("2017-11-06", "zhangyanpeng")
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .getOrCreate()

    //获取-d参数
    val yesterday = TimeUtil.processArgs(args)

    //    val df = Aldstat_args_tool.analyze_args(args,spark,DBConf.hdfsUrl).filter("ev='event' and tp!='null'")
    val df = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet")).filter("ev='event' and tp!='null'")

    df.createTempView("daily_event")

    //读取mysql 中 的数据（ald_event）
    val event_df = JdbcUtil.readFromMysql(spark, "(select app_key,ev_id,event_key,ev_name from ald_event) as event_df")

    //触发的次数     触发用户数
    val dataFrame = spark.sql("select ak,tp,count(distinct(uu)) count_uu,count(at) count_at,cast(count(at)/count(distinct(uu)) as float) trigger_user_count,now() time from daily_event group by ak,tp").distinct()

    //把读取数据库的数据和读取日志的数据进行join 条件是日志的tp = 数据库的ev_id
    val result = dataFrame.join(event_df, dataFrame("ak") === event_df("app_key") &&
      dataFrame("tp") === event_df("ev_id"))
      .select(
        event_df("app_key"),
        event_df("ev_id"),
        event_df("event_key"),
        dataFrame("count_uu"),
        dataFrame("count_at"),
        dataFrame("time"),
        dataFrame("trigger_user_count")
      )

    var statement: Statement = null
    result.foreachPartition((row: Iterator[Row]) => {
      //连接
      //      val conn =MySqlPool.getJdbcConn()
      //      statement = conn.createStatement()
      //      try {
      //      row.foreach(r =>{
      //        conn.setAutoCommit(false)
      //
      //        val app_key=r(0)
      //        val day=yesterday
      //        val ev_id=r(1)
      //        val event_key=r(2)
      //        val trigger_user_count=r(3)    //触发人数
      //        val trigger_count=r(4)         //触发次数
      //        val update_at=r(5)
      //        val avg_trigger_count=r(6)      //人均触发数
      //
      //        val sql = "insert into aldstat_30days_event (app_key,day,ev_id,event_key,trigger_user_count,trigger_count,avg_trigger_count,update_at)"+s"values ('${app_key}','${day}','${ev_id}','${event_key}','${trigger_user_count}','${trigger_count}','${avg_trigger_count}','${update_at}') ON DUPLICATE KEY UPDATE trigger_user_count='${trigger_user_count}', trigger_count='${trigger_count}',avg_trigger_count='${avg_trigger_count}',update_at='${update_at}'"
      //        statement.addBatch(sql)
      //      })
      //      statement.executeBatch
      //      conn.commit()
      //
      //      } catch {
      //        case e: Exception => e.printStackTrace()
      //          conn.close()
      //      }
      //      val conn = JdbcUtil.getConn()
      //      val statement = conn.createStatement
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_30days_event (app_key,day,ev_id,event_key,trigger_user_count,trigger_count,avg_trigger_count,update_at)" + s"values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE trigger_user_count=?, trigger_count=?,avg_trigger_count=?,update_at=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = yesterday
        val ev_id = r.get(1)
        val event_key = r.get(2)
        val trigger_user_count = r.get(3) //触发人数
        val trigger_count = r.get(4) //触发次数
        val update_at = r.get(5)
        val avg_trigger_count = r.get(6)
        //          ON DUPLICATE KEY UPDATE trigger_user_count='${trigger_user_count}', trigger_count='${trigger_count}',avg_trigger_count='${avg_trigger_count}',
        // update_at='${update_at}'"
        params.+=(Array[Any](app_key, day, ev_id, event_key, trigger_user_count, trigger_count, avg_trigger_count, update_at, trigger_user_count, trigger_count, avg_trigger_count, update_at))

      })
      //      try {
      JdbcUtil.doBatch(sqlText, params) //批量入库
      //      }
      //      finally {
      //        statement.close() //关闭statement
      //        conn.close() //关闭数据库连接
      //      }
    })

    spark.stop()
  }
}
