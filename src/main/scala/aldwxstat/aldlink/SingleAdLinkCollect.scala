package aldwxstat.aldlink

import java.sql.Timestamp

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangtaiyang on 2017/12/14. 
  */


class SingleAdLinkCollect {

  /**<br>gcs:<br>
    * 这个函数是什么功能啊？ <br>
    * 这个函数会从 visitora_page 这个视图当中将数据提取出来一部分之后创建一个df <br>
    * 之后从df中将数据提取出来之后，插入到aldstat_${numDay}days_single_link 表当中。numDay 可以取7，或者numDays取30
    * @param spark 用于插入的sparkSession
    * @param numDay 这个天数可以是7和30天
    * */
  def linkSevenCollect(spark: SparkSession, numDay: String): Unit = {
    val day: String = ArgsTool.day //gcs:将-d 后面的操作读取出来

    //gcs:这个visitora_page 是什么DF啊？反正是从
    val df = spark.sql("SELECT ak app_key,ag_ald_link_key,COUNT(DISTINCT uu) visitor_count FROM visitora_page GROUP BY ak,ag_ald_link_key")

    //gcs:从df中提取出来一些字段，之后存储到数据库当中
    df.na.fill("0").na.fill(0).foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        s"""
          |insert into aldstat_${numDay}days_single_link(app_key,day,link_key,link_visitor_count)values(?,?,?,?)
          |ON DUPLICATE KEY UPDATE link_visitor_count=?
        """.stripMargin

      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val appKey = row.get(0).toString
        val linkId = row.get(1).toString
        val visitorCount = row.get(2).toString

        params.+=(Array[Any](appKey, day, linkId, visitorCount,visitorCount))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库
    })

  }

  /**<br>gcs:<br>
    * 从视图 visitora_page 中提取出来，之后将数据存储到数据库 aldstat_${numDay}days_single_media 中
    * */
  def mediaSevenCollect(spark: SparkSession, numDay: String): Unit = {

    //==========================================================1
    /*
    *gcs:
    *将-d 后面的参数day提取出来
    */
    val day: String = ArgsTool.day

    //==========================================================2
    /*
    *gcs:
    *使用视图 visitora_page 提取一些列的数据参数
    */
    spark.sql("SELECT ak app_key,ag_ald_media_id,COUNT(DISTINCT uu) visitor_count FROM visitora_page GROUP BY ak,ag_ald_media_id")


      //==========================================================3
      /*
      *gcs:
      *将数据插入到aldstat_${numDay}days_single_media 数据库
      */
      .na.fill(0).na.fill("0").foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        s"""
          |insert into aldstat_${numDay}days_single_media(app_key,day,media_id,media_visitor_count)values(?,?,?,?)
          |ON DUPLICATE KEY UPDATE media_visitor_count=?
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val appKey = row.get(0).toString
        val mediaId = row.get(1).toString
        val visitorCount = row.get(2).toString

        params.+=(Array[Any](appKey, day, mediaId, visitorCount,visitorCount))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库
    })
  }

  def positionSevenCollect(spark: SparkSession, numDay: String): Unit = {
    val day: String = ArgsTool.day
    spark.sql("SELECT ak app_key,ag_ald_position_id,COUNT(DISTINCT uu) visitor_count FROM visitora_page GROUP BY ak,ag_ald_position_id")
      .na.fill(0).na.fill("0").foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        s"""
          |insert into aldstat_${numDay}days_single_position(app_key,day,position_id,position_visitor_count)values(?,?,?,?)
          |ON DUPLICATE KEY UPDATE position_visitor_count=?
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val appKey = row.get(0).toString
        val positionId = row.get(1).toString
        val visitorCount = row.get(2).toString

        params.+=(Array[Any](appKey, day, positionId, visitorCount,visitorCount))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库

    })
  }


  /**<br>gcs:<br>
    * 这是分析7天和30天的数据吗? <br>
    * */
  def executeSingleSevenOrThirty(spark: SparkSession, numDays: String): Unit = {

    //    val logs = ArgsTool.getSevenOrThirtyDF(spark, DBConf.hdfsUrl, numDays)
    //==========================================================1
    /*
    *gcs:
    *读取7天或者30天的log日志数据
    */
    val logs = ArgsTool.getSevenOrThirtyDF(spark, ConfigurationUtil.getProperty("tongji.parquet"), numDays)

    //val logs: DataFrame = spark.read.parquet("/taiyang/link/20171214/etl-ald-log-2-2017121416")
    println(s"开始处理单个${numDays}天的数据")
    //==========================================================2
    /*
    *gcs:
    *将数据进行筛选之后，创建视图 visitora_page
    */
    logs.filter("ev='page' and ag_ald_link_key!='null' and ag_ald_position_id!='null' and ag_ald_media_id!='null' and scene!='null'")
      .select("ak", "ag_ald_link_key", "ag_ald_media_id", "ag_ald_position_id", "uu", "hour", "at", "dr", "pp","scene")
      .createOrReplaceTempView("visitora_page")

    //    spark.sql(
    //      """
    //        |select ak,hour,del_link.link_key ag_ald_link_key,ag_ald_media_id,ag_ald_position_id,uu,at,dr,pp
    //        |from visitora_page vp
    //        |right join (select link_key from global_temp.ald_link_trace where is_del=0) del_link
    //        |on vp.ag_ald_link_key=del_link.link_key
    //      """.stripMargin).distinct()
    //      .createOrReplaceTempView("visitora_page")

    //==========================================================3
    /*
    *gcs:
    *进行另外的一系列的筛选，之后创建一个visitora_page视图
    */
    spark.sql(
      """
        |select trace.link_key ag_ald_link_key,trace.app_key ak,trace.media_id ag_ald_media_id,
        |case when scene in (1058,1035,1014,1038) then scene else '其它' end as ag_ald_position_id,
        |vp.hour,vp.uu,vp.at,vp.dr,vp.pp
        |from
        |(
        |select link_key,app_key,media_id
        |from global_temp.ald_link_trace
        |where is_del=0
        |) trace
        |left join visitora_page vp
        |on vp.ag_ald_link_key=trace.link_key
      """.stripMargin)
      .createOrReplaceTempView("visitora_page")

    //==========================================================4
    /*
    *gcs:
    *将视图进行cache的操作
    */
    spark.sqlContext.cacheTable("visitora_page")

    //==========================================================5
    /*
    *gcs:
    *使用刚刚创建好的 visitora_page 视图进行数据的分析
    */
    linkSevenCollect(spark, numDays)

    //==========================================================6
    /*
    *gcs:
    *
    */
    mediaSevenCollect(spark, numDays)
    positionSevenCollect(spark, numDays)

  }
}
