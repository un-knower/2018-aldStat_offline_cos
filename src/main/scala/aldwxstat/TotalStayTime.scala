package aldwxstat

import java.sql.Statement

import aldwxconfig.ConfigurationUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import aldwxutils.{JdbcUtil, TimeUtil}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangyanpeng on 2017/8/4.
  * 场景值 总停留时长
  */
object TotalStayTime {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .getOrCreate()

    // 获取当天的前一个小时
    val hour = TimeUtil.processArgsStayTimeHour(args)
    // 获取今天日期
    val dateStr = TimeUtil.processArgsStayTimeDay(args)
    // 当前事件的 unix 时间戳
    val timeStr = TimeUtil.getTimeString()

    // 打印调试信息
    println("dateStr: " + dateStr)
    println("timeStr " + timeStr)
    //    println(s"${DBConf.hdfsUrl}/${dateStr}/etl-*${dateStr}${hour}/part-*")
    println(s"${ConfigurationUtil.getProperty("hdfsUrl")}/${dateStr}/etl-*${dateStr}${hour}/part-*")

    //==========================================================1
    /*
    *gcs:
    *从数据库中读取数据
    */
    //读取 场景值的组列表
    val scene_df = JdbcUtil.readFromMysql(spark, "(select sid, scene_group_id from ald_cms_scene) as scene_df")

    //==========================================================2
    /*
    *gcs:
    *创建视图
    */
    scene_df.createTempView("scene_df")

    //小时 级别
    val df = spark.read.parquet(s"${ConfigurationUtil.getProperty("hdfsUrl")}/${dateStr}/etl-*${dateStr}${hour}/part-*").filter("ev='page' and ak !=''").repartition(3).cache()
    df.createTempView("stayTime_hour")


    //==========================================================3
    /*
    *gcs:
    *就算场景值的时间大小
    */
    //单个场景值的 小时 停留时长
    val rs_1 = spark.sql("SELECT ak,scene,sum(time) total_time FROM (SELECT ak,scene,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) time FROM stayTime_hour GROUP BY ak,scene,at) group by ak,scene").distinct()
    rs_1.createTempView("scene_hour")


    //==========================================================4
    /*
    *gcs:
    *将两个表进行左连接，之后将连接的结果
    */
    //场景值组 小时 停留时长
    spark.sql("select sh.ak ak ,sh.scene scene ,sd.scene_group_id group_id ,sh.total_time time from scene_hour sh left join scene_df sd on sd.sid=sh.scene").createTempView("group_hour")
    val rs_2 = spark.sql("select ak,group_id,sum(time) from group_hour group by ak,group_id").distinct()

    //趋势分析  小时统计
    val trend_1 = spark.sql("SELECT ak,sum(time) total_time FROM (SELECT ak,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) time FROM stayTime_hour GROUP BY ak,at) group by ak").distinct()

    //天级别

    val df_1 = spark.read.parquet(s"${ConfigurationUtil.getProperty("hdfsUrl")}/${dateStr}/*/part-*").filter("ev='page' and ak !=''").repartition(50).cache()
    df_1.createTempView("stayTime_day")


    //单个场景值  天  停留时长
    val rs_3 = spark.sql("SELECT ak,scene,sum(time) total_time FROM (SELECT ak,scene,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) time FROM stayTime_day GROUP BY ak,scene,at) group by ak,scene").distinct()
    rs_3.createTempView("scene_day")
    //场景值组 天 停留时长
    spark.sql("select sh.ak ak ,sh.scene scene ,sd.scene_group_id group_id ,sh.total_time time from scene_day sh left join scene_df sd on sd.sid=sh.scene").createTempView("group_day")
    val rs_4 = spark.sql("select ak,group_id,sum(time) from group_day group by ak,group_id").distinct()
    //趋势分析   天 统计
    val trend_2 = spark.sql("SELECT ak,sum(time) total_time,UNIX_TIMESTAMP(now()) FROM (SELECT ak,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) time FROM stayTime_day GROUP BY ak,at) group by ak").distinct()

    var statement: Statement = null

    rs_1.foreachPartition((row: Iterator[Row]) => {
      //连接

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_hourly_scene(`app_key`,`day`,`hour`,`scene_id`,`total_stay_time`)" + s" values(?,?,?,?,?) " +
        s"ON DUPLICATE KEY UPDATE total_stay_time=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        val scene = r.get(1)
        val stay_time = r.get(2)

        params.+=(Array[Any](`app_key`, `day`, `hour`, `scene`, `stay_time`, stay_time))
      })
      JdbcUtil.doBatch(sqlText, params)
    })

    rs_2.foreachPartition((row: Iterator[Row]) => {
      //连接

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_hourly_scene_group(`app_key`,`day`,`hour`,`scene_group_id`,`total_stay_time`)" + s" values(?,?,?,?,?) " +
        s"ON DUPLICATE KEY UPDATE total_stay_time=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        var group_id = r.get(1)
        if (group_id == null) {
          group_id = 11
        }
        val stay_time = r.get(2)

        params.+=(Array[Any](`app_key`, `day`, `hour`, `group_id`, `stay_time`, stay_time))

      })
      JdbcUtil.doBatch(sqlText, params)
    })

    rs_3.foreachPartition((row: Iterator[Row]) => {

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_scene_statistics(`app_key`,`day`,`scene_id`,`total_stay_time`,`update_at`)" + s" values(?,?,?,?,?) " +
        s"ON DUPLICATE KEY UPDATE total_stay_time=?,update_at=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        val scene = r.get(1)
        val stay_time = r.get(2)

        params.+=(Array[Any](`app_key`, `day`, `scene`, `stay_time`, `timeStr`, stay_time, timeStr))

      })
      JdbcUtil.doBatch(sqlText,params)
    })

    rs_4.foreachPartition((row: Iterator[Row]) => {

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_daily_scene_group(`app_key`,`day`,`scene_group_id`,`total_stay_time`)" +
        s" values(?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        var group_id = r.get(1)
        if (group_id == null) {
          group_id = 11
        }

        val stay_time = r.get(2)

        params.+=(Array[Any](`app_key`, `day`, `group_id`, `stay_time`, stay_time))

      })
      JdbcUtil.doBatch(sqlText,params)
    })

    trend_1.foreachPartition((row: Iterator[Row]) => {

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_hourly_trend_analysis(`app_key`,`day`,`hour`,`total_stay_time`)" + s" values(?,?,?,?') " +
        s"ON DUPLICATE KEY UPDATE total_stay_time=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        val stay_time = r.get(1)

        params.+=(Array[Any](`app_key`, `day`, `hour`, `stay_time`, stay_time))

      })
      JdbcUtil.doBatch(sqlText,params)
    })

    trend_2.foreachPartition((row: Iterator[Row]) => {

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_trend_analysis(`app_key`,`day`,`total_stay_time`,`update_at`)" + s" values(?,?,?,?) " +
        s"ON DUPLICATE KEY UPDATE total_stay_time=?,update_at=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        val stay_time = r.get(1)
        val update_at = r.get(2)

        params.+=(Array[Any](`app_key`, `day`, `stay_time`, `update_at`, stay_time, update_at))

      })
      JdbcUtil.doBatch(sqlText,params)
    })

    spark.stop()
  }
}
