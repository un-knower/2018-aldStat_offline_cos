package aldwxstat.aldlink

import java.sql.Timestamp

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, DateTimeUtil, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangtaiyang on 2017/12/25.
  */
object TodayStayTimeLink {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .getOrCreate()

    //==========================================================1
    /*
    *gcs:
    *从 ald_link_trace 表中把数据读取出来。之后创建一个临时的全局的视图 ald_link_trace
    */
    println("创建全局管理表ald_link_trace")
    JdbcUtil.readFromMysql(spark, "ald_link_trace").createGlobalTempView("ald_link_trace")
    spark.sqlContext.cacheTable("global_temp.ald_link_trace")

    //==========================================================2
    /*
    *gcs:
    *将表中的参数都读取出来
    */
    ArgsTool.analysisArgs(args)

    //==========================================================3
    fillDailyData(spark)

    spark.close()
  }


  /**<br>gcs:<br>
    * 这个是数据处理的主要的函数
    * */
  def fillDailyData(spark: SparkSession): Unit = {
    //    val logs = ArgsTool.getDailyDataFrame(spark, DBConf.hdfsUrl)

    //==========================================================4
    /*
    *gcs:
    *从path路径下获得-du天的数据
    */
    val logs = ArgsTool.getDailyDataFrame(spark, ConfigurationUtil.getProperty("tongji.parquet"))
    //val logs = spark.read.parquet("/taiyang/link/20171214/etl-ald-log-2-2017121416")

    println("创建原始日志视图...")
    logs.createOrReplaceTempView("logs") //gcs:把刚刚读取出来的logs数据生成一个视图

    //==========================================================5
    /*
    *gcs:
    *开始就算
    */
    //每个指标求停留时长时需要用到这个表
    spark.sql("select ak,uu,at,dr,ifo,hour from logs where ev='app'").createOrReplaceTempView("app_tmp")
    spark.sqlContext.cacheTable("app_tmp")
    //求出ev=app的ak，uu，每个指标算新用户时用到  ifo
    spark.sql("select ak,uu from app_tmp where ifo='true' group by ak,uu").createOrReplaceTempView("nu_app_daily")
    spark.sqlContext.cacheTable("nu_app_daily")
    spark.sql("select ak,uu,hour from app_tmp where ifo='true' group by ak,uu,hour").createOrReplaceTempView("nu_app_hourly")
    spark.sqlContext.cacheTable("nu_app_hourly")


    println("创建visitora_page...")
    logs.filter("ev='page' and ag_ald_link_key!='null' and ag_ald_position_id!='null' and ag_ald_media_id!='null'")
      .createOrReplaceTempView("visitora_page")

    //==========================================================6
    /*
    *gcs:
    *这下面写的Sql语句使用了join语句。这些语法用的比较好
    */
    spark.sql(
      """
        |select trace.link_key ag_ald_link_key,trace.app_key ak,trace.media_id ag_ald_media_id,trace.media_position ag_ald_position_id,
        |vp.hour,vp.uu,vp.at,vp.dr,vp.pp
        |from
        |(
        |select link_key,app_key,media_id,media_position
        |from global_temp.ald_link_trace
        |where is_del=0
        |) trace
        |left join visitora_page vp
        |on vp.ag_ald_link_key=trace.link_key
      """.stripMargin)
      .createOrReplaceTempView("visitora_page")
    spark.sqlContext.cacheTable("visitora_page")

    new TodayStayTime(spark).insert2db()
    new TodayLinkStayTime(spark).insert2db()
    new TodayMediaStayTime(spark).insert2db()
    new TodayPositionStayTime(spark).insert2db()
    new HourlyLinkStayTime(spark).insert2db()
    new HourlyMediaStayTime(spark).insert2db()
    new HourlyPositionStayTime(spark).insert2db()

    spark.sqlContext.uncacheTable("app_tmp")
    spark.sqlContext.uncacheTable("nu_app_daily")
    spark.sqlContext.uncacheTable("nu_app_hourly")
    spark.sqlContext.uncacheTable("visitora_page")

    spark.close()
  }
}

class TodayStayTime(spark: SparkSession) {
  def insert2db(): Unit = {
    spark.sql("SELECT ak app_key,COUNT(DISTINCT at) open_count FROM visitora_page GROUP BY ak")
      .createOrReplaceTempView("open_count")
    spark.sql(
      """
        |select app_key,sum(dr)/1000.0 total_stay_time from
        |(
        |select page.ak app_key,page.dr dr
        |from visitora_page page
        |left join app_tmp app
        |on page.ak=app.ak and page.at=app.at
        |)
        |group by app_key
      """.stripMargin).createOrReplaceTempView("total_stay_time")
    spark.sql(
      """
        |SELECT oc.app_key,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
        |FROM open_count oc
        |left join total_stay_time tst
        |on oc.app_key=tst.app_key
      """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")

    val day = new DateTimeUtil().getUdfDate(0, "yyyy-MM-dd")
    val linkDailyDetail: DataFrame = spark.sql(
      """
        |select vc.app_key,tst.total_stay_time,sast.secondary_avg_stay_time
        |from open_count vc
        |left join total_stay_time tst
        |on vc.app_key=tst.app_key
        |left join secondary_avg_stay_time sast
        |on vc.app_key=sast.app_key
      """.stripMargin) //.show()
    println("计算完成准备入库...")
    linkDailyDetail.na.fill("0").na.fill(0).foreachPartition(rows => {
      //      val conn = DBConf.getConn
      //      var ps: PreparedStatement = null
      //      conn.setAutoCommit(false)
      //      var count = 0
      //
      //      val update_at = new Timestamp(System.currentTimeMillis()).toString
      //      rows.foreach(row => {
      //        val app_key = row.get(0).toString
      //        val total_stay_time = row.get(1).toString
      //        val secondary_avg_stay_time = row.get(2).toString
      //
      //        val sql = MessageFormat.format(
      //          """
      //            |insert into aldstat_link_summary(app_key,day,total_stay_time,secondary_stay_time,update_at)
      //            |values("{0}","{1}","{2}","{3}","{4}")
      //            |ON DUPLICATE KEY UPDATE total_stay_time=values(total_stay_time),secondary_stay_time=values(secondary_stay_time)
      //          """.stripMargin, app_key, day, total_stay_time, secondary_avg_stay_time, update_at)
      //
      //        ps = conn.prepareStatement(sql)
      //        if (count < 500) {
      //          ps.addBatch()
      //          count += 1
      //        } else {
      //          ps.executeBatch()
      //          ps.clearBatch()
      //          count = 0
      //        }
      //      })
      //      if (ps != null) {
      //        ps.executeBatch()
      //        conn.commit()
      //
      //        ps.close()
      //        conn.close()
      //      }
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        """
          |insert into aldstat_link_summary(app_key,day,total_stay_time,secondary_stay_time,update_at)
          |values(?,?,?,?,?)
          |ON DUPLICATE KEY UPDATE total_stay_time=values(total_stay_time),secondary_stay_time=values(secondary_stay_time)
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val app_key = row.get(0).toString
        val total_stay_time = row.get(1).toString
        val secondary_avg_stay_time = row.get(2).toString

        params.+=(Array[Any](app_key, day, total_stay_time, secondary_avg_stay_time, update_at))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库

    })
  }

}

class TodayLinkStayTime(spark: SparkSession) {

  def insert2db(): Unit = {
    val day = new DateTimeUtil().getUdfDate(0, "yyyy-MM-dd")

    spark.sql("SELECT ak app_key,ag_ald_link_key,COUNT(DISTINCT at) open_count FROM visitora_page GROUP BY ak,ag_ald_link_key")
      .createOrReplaceTempView("open_count")
    spark.sql(
      """
        |select app_key,ag_ald_link_key,sum(dr)/1000.0 total_stay_time from
        |(
        |select page.ak app_key,page.ag_ald_link_key ag_ald_link_key,page.dr dr
        |from visitora_page page
        |left join app_tmp app
        |on page.ak=app.ak and page.at=app.at
        |)
        |group by app_key,ag_ald_link_key
      """.stripMargin).createOrReplaceTempView("total_stay_time")

    spark.sql(
      """
        |SELECT oc.app_key,oc.ag_ald_link_key,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
        |FROM open_count oc
        |left join total_stay_time tst
        |on oc.app_key=tst.app_key and oc.ag_ald_link_key=tst.ag_ald_link_key
      """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")

    val linkDailyDetail = spark.sql(
      """
        |select vc.app_key,vc.ag_ald_link_key,tst.total_stay_time,sast.secondary_avg_stay_time
        |from open_count vc
        |left join total_stay_time tst
        |on vc.app_key=tst.app_key and vc.ag_ald_link_key=tst.ag_ald_link_key
        |left join secondary_avg_stay_time sast
        |on vc.app_key=sast.app_key and vc.ag_ald_link_key=sast.ag_ald_link_key
      """.stripMargin) //.show()

    linkDailyDetail.na.fill("0").na.fill(0).foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        """
          |insert into aldstat_daily_link(app_key,day,link_key,total_stay_time,secondary_stay_time,update_at)
          |values(?,?,?,?,?,?)
          |ON DUPLICATE KEY UPDATE total_stay_time=values(total_stay_time),secondary_stay_time=values(secondary_stay_time)
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val app_key = row.get(0).toString
        val ag_ald_link_key = row.get(1).toString
        val total_stay_time = row.get(2).toString
        val secondary_avg_stay_time = row.get(3).toString

        params.+=(Array[Any](app_key, day, ag_ald_link_key, total_stay_time, secondary_avg_stay_time, update_at))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库
    })
  }

}

class TodayMediaStayTime(spark: SparkSession) {

  //外链每日详情入库
  def insert2db(): Unit = {
    val day = new DateTimeUtil().getUdfDate(0, "yyyy-MM-dd")
    spark.sql("SELECT ak app_key,ag_ald_media_id,COUNT(DISTINCT at) open_count FROM visitora_page GROUP BY ak,ag_ald_media_id")
      .createOrReplaceTempView("open_count")
    spark.sql(
      """
        |select app_key,ag_ald_media_id,sum(dr)/1000.0 total_stay_time from
        |(
        |select page.ak app_key,page.ag_ald_media_id ag_ald_media_id,page.dr dr
        |from visitora_page page
        |left join app_tmp app
        |on page.ak=app.ak and page.at=app.at
        |)
        |group by app_key,ag_ald_media_id
      """.stripMargin).createOrReplaceTempView("total_stay_time")
    spark.sql(
      """
        |SELECT oc.app_key,oc.ag_ald_media_id,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
        |FROM open_count oc
        |left join total_stay_time tst
        |on oc.app_key=tst.app_key and oc.ag_ald_media_id=tst.ag_ald_media_id
      """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
    val linkDailyDetail = spark.sql(
      """
        |select vc.app_key,vc.ag_ald_media_id,tst.total_stay_time,sast.secondary_avg_stay_time
        |from open_count vc
        |left join total_stay_time tst
        |on vc.app_key=tst.app_key and vc.ag_ald_media_id=tst.ag_ald_media_id
        |left join secondary_avg_stay_time sast
        |on vc.app_key=sast.app_key and vc.ag_ald_media_id=sast.ag_ald_media_id
      """.stripMargin) //.show()

    linkDailyDetail.na.fill("0").na.fill(0).foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        """
          |insert into aldstat_daily_media(app_key,day,media_id,total_stay_time,secondary_stay_time,update_at)
          |values(?,?,?,?,?,?)
          |ON DUPLICATE KEY UPDATE total_stay_time=values(total_stay_time),secondary_stay_time=values(secondary_stay_time)
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val app_key = row.get(0).toString
        val ag_ald_media_id = row.get(1).toString
        val total_stay_time = row.get(2).toString
        val secondary_avg_stay_time = row.get(3).toString

        params.+=(Array[Any](app_key, day, ag_ald_media_id, total_stay_time, secondary_avg_stay_time, update_at))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库
    })
  }

}

class TodayPositionStayTime(spark: SparkSession) {

  //外链每日详情入库
  def insert2db(): Unit = {
    spark.sql("SELECT ak app_key,ag_ald_position_id,COUNT(DISTINCT at) open_count FROM visitora_page GROUP BY ak,ag_ald_position_id")
      .createOrReplaceTempView("open_count")
    spark.sql(
      """
        |select app_key,ag_ald_position_id,sum(dr)/1000.0 total_stay_time from
        |(
        |select page.ak app_key,page.ag_ald_position_id ag_ald_position_id,page.dr dr
        |from visitora_page page
        |left join app_tmp app
        |on page.ak=app.ak and page.at=app.at
        |)
        |group by app_key,ag_ald_position_id
      """.stripMargin).createOrReplaceTempView("total_stay_time")
    spark.sql(
      """
        |SELECT oc.app_key,oc.ag_ald_position_id,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
        |FROM open_count oc
        |left join total_stay_time tst
        |on oc.app_key=tst.app_key and oc.ag_ald_position_id=tst.ag_ald_position_id
      """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
    val day = new DateTimeUtil().getUdfDate(0, "yyyy-MM-dd")

    val linkDailyDetail = spark.sql(
      """
        |select vc.app_key,vc.ag_ald_position_id,tst.total_stay_time,sast.secondary_avg_stay_time
        |from open_count vc
        |left join total_stay_time tst
        |on vc.app_key=tst.app_key and vc.ag_ald_position_id=tst.ag_ald_position_id
        |left join secondary_avg_stay_time sast
        |on vc.app_key=sast.app_key and vc.ag_ald_position_id=sast.ag_ald_position_id
      """.stripMargin) //.show()

    linkDailyDetail.na.fill("0").na.fill(0).foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        """
          |insert into aldstat_daily_position(app_key,day,position_id,total_stay_time,secondary_stay_time,update_at)
          |values(?,?,?,?,?,?)
          |ON DUPLICATE KEY UPDATE total_stay_time=values(total_stay_time),secondary_stay_time=values(secondary_stay_time)
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val app_key = row.get(0).toString
        val ag_ald_position_id = row.get(1).toString
        val total_stay_time = row.get(2).toString
        val secondary_avg_stay_time = row.get(3).toString

        params.+=(Array[Any](app_key, day, ag_ald_position_id, total_stay_time, secondary_avg_stay_time, update_at))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库
    })
  }

}

class HourlyLinkStayTime(spark: SparkSession) {
  //外链小时详情入库
  def insert2db(): Unit = {
    spark.sql("SELECT ak app_key,hour,ag_ald_link_key,COUNT(DISTINCT at) open_count FROM visitora_page GROUP BY ak,hour,ag_ald_link_key")
      .createOrReplaceTempView("open_count")
    spark.sql("SELECT ak app_key,hour,ag_ald_link_key,COUNT(pp) total_page_count FROM visitora_page GROUP BY ak,hour,ag_ald_link_key")
      .createOrReplaceTempView("total_page_count")
    spark.sql(
      """
        |select app_key,hour,ag_ald_link_key,sum(dr)/1000.0 total_stay_time from
        |(
        |select page.ak app_key,page.hour hour,page.ag_ald_link_key ag_ald_link_key,page.dr dr
        |from visitora_page page
        |left join app_tmp app
        |on page.ak=app.ak and page.at=app.at and page.hour=app.hour
        |)
        |group by app_key,hour,ag_ald_link_key
      """.stripMargin).createOrReplaceTempView("total_stay_time")
    spark.sql(
      """
        |SELECT oc.app_key app_key,oc.hour hour,oc.ag_ald_link_key,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
        |FROM open_count oc
        |left join total_stay_time tst
        |on oc.app_key=tst.app_key and oc.hour=tst.hour and oc.ag_ald_link_key=tst.ag_ald_link_key
      """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
    val day = new DateTimeUtil().getUdfDate(0, "yyyy-MM-dd")
    val linkDailyDetail = spark.sql(
      """
        |select vc.app_key,vc.hour,vc.ag_ald_link_key,tst.total_stay_time,sast.secondary_avg_stay_time
        |from open_count vc
        |left join total_stay_time tst
        |on vc.app_key=tst.app_key and vc.hour=tst.hour and vc.ag_ald_link_key=tst.ag_ald_link_key
        |left join secondary_avg_stay_time sast
        |on vc.app_key=sast.app_key and vc.hour=sast.hour and vc.ag_ald_link_key=sast.ag_ald_link_key
      """.stripMargin) //.show()

    linkDailyDetail.na.fill("0").na.fill(0).foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        """
          |insert into aldstat_hourly_link(app_key,day,hour,link_key,total_stay_time,secondary_stay_time,update_at)
          |values(?,?,?,?,?,?,?)
          |ON DUPLICATE KEY UPDATE total_stay_time=values(total_stay_time),secondary_stay_time=values(secondary_stay_time)
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val app_key = row.get(0).toString
        val hour = row.get(1) toString
        val ag_ald_link_key = row.get(2) toString
        val total_stay_time = row.get(3) toString
        val secondary_avg_stay_time = row.get(4) toString

        params.+=(Array[Any](app_key, day, hour, ag_ald_link_key, total_stay_time, secondary_avg_stay_time, update_at))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库

    })
  }
}

class HourlyMediaStayTime(spark: SparkSession) {
  //外链小时详情入库
  def insert2db(): Unit = {
    spark.sql("SELECT ak app_key,hour,ag_ald_media_id,COUNT(DISTINCT at) open_count FROM visitora_page GROUP BY ak,hour,ag_ald_media_id")
      .createOrReplaceTempView("open_count")
    spark.sql("SELECT ak app_key,hour,ag_ald_media_id,COUNT(pp) total_page_count FROM visitora_page GROUP BY ak,hour,ag_ald_media_id")
      .createOrReplaceTempView("total_page_count")
    spark.sql(
      """
        |select app_key,hour,ag_ald_media_id,sum(dr)/1000.0 total_stay_time from
        |(
        |select page.ak app_key,page.hour hour,page.ag_ald_media_id ag_ald_media_id,page.dr dr
        |from visitora_page page
        |left join app_tmp app
        |on page.ak=app.ak and page.at=app.at and page.hour=app.hour
        |)
        |group by app_key,hour,ag_ald_media_id
      """.stripMargin).createOrReplaceTempView("total_stay_time")
    spark.sql(
      """
        |SELECT oc.app_key app_key,oc.hour hour,oc.ag_ald_media_id,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
        |FROM open_count oc
        |left join total_stay_time tst
        |on oc.app_key=tst.app_key and oc.hour=tst.hour and oc.ag_ald_media_id=tst.ag_ald_media_id
      """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
    val day = new DateTimeUtil().getUdfDate(0, "yyyy-MM-dd")
    val linkDailyDetail = spark.sql(
      """
        |select vc.app_key,vc.hour,vc.ag_ald_media_id,tst.total_stay_time,sast.secondary_avg_stay_time
        |from open_count vc
        |left join total_stay_time tst
        |on vc.app_key=tst.app_key and vc.hour=tst.hour and vc.ag_ald_media_id=tst.ag_ald_media_id
        |left join secondary_avg_stay_time sast
        |on vc.app_key=sast.app_key and vc.hour=sast.hour and vc.ag_ald_media_id=sast.ag_ald_media_id
      """.stripMargin) //.show()

    linkDailyDetail.na.fill("0").na.fill(0).foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        """
          |insert into aldstat_hourly_media(app_key,day,hour,media_id,total_stay_time,secondary_stay_time,update_at)
          |values(?,?,?,?,?,?,?)
          |ON DUPLICATE KEY UPDATE total_stay_time=values(total_stay_time),secondary_stay_time=values(secondary_stay_time)
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val app_key = row.get(0).toString
        val hour = row.get(1) toString
        val ag_ald_media_id = row.get(2) toString
        val total_stay_time = row.get(3) toString
        val secondary_avg_stay_time = row.get(4) toString

        params.+=(Array[Any](app_key, day, hour, ag_ald_media_id, total_stay_time, secondary_avg_stay_time, update_at))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库
    })
  }
}

class HourlyPositionStayTime(spark: SparkSession) {
  //外链小时详情入库
  def insert2db(): Unit = {
    spark.sql("SELECT ak app_key,hour,ag_ald_position_id,COUNT(DISTINCT at) open_count FROM visitora_page GROUP BY ak,hour,ag_ald_position_id")
      .createOrReplaceTempView("open_count")
    spark.sql("SELECT ak app_key,hour,ag_ald_position_id,COUNT(pp) total_page_count FROM visitora_page GROUP BY ak,hour,ag_ald_position_id")
      .createOrReplaceTempView("total_page_count")
    spark.sql(
      """
        |select app_key,hour,ag_ald_position_id,sum(dr)/1000.0 total_stay_time from
        |(
        |select page.ak app_key,page.hour hour,page.ag_ald_position_id ag_ald_position_id,page.dr dr
        |from visitora_page page
        |left join app_tmp app
        |on page.ak=app.ak and page.at=app.at and page.hour=app.hour
        |)
        |group by app_key,hour,ag_ald_position_id
      """.stripMargin).createOrReplaceTempView("total_stay_time")
    spark.sql(
      """
        |SELECT oc.app_key app_key,oc.hour hour,oc.ag_ald_position_id,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
        |FROM open_count oc
        |left join total_stay_time tst
        |on oc.app_key=tst.app_key and oc.hour=tst.hour and oc.ag_ald_position_id=tst.ag_ald_position_id
      """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
    val day = new DateTimeUtil().getUdfDate(0, "yyyy-MM-dd")
    val linkDailyDetail = spark.sql(
      """
        |select vc.app_key,vc.hour,vc.ag_ald_position_id,tst.total_stay_time,sast.secondary_avg_stay_time
        |from open_count vc
        |left join total_stay_time tst
        |on vc.app_key=tst.app_key and vc.hour=tst.hour and vc.ag_ald_position_id=tst.ag_ald_position_id
        |left join secondary_avg_stay_time sast
        |on vc.app_key=sast.app_key and vc.hour=sast.hour and vc.ag_ald_position_id=sast.ag_ald_position_id
      """.stripMargin) //.show()

    linkDailyDetail.na.fill("0").na.fill(0).foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        """
          |insert into aldstat_hourly_position(app_key,day,hour,position_id,total_stay_time,secondary_stay_time,update_at)
          |values(?,?,?,?,?,?,?)
          |ON DUPLICATE KEY UPDATE total_stay_time=values(total_stay_time),secondary_stay_time=values(secondary_stay_time)
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val app_key = row.get(0).toString
        val hour = row.get(1) toString
        val ag_ald_position_id = row.get(2) toString
        val total_stay_time = row.get(3) toString
        val secondary_avg_stay_time = row.get(4) toString

        params.+=(Array[Any](app_key, day, hour, ag_ald_position_id, total_stay_time, secondary_avg_stay_time, update_at))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库
    })
  }
}
