//==========================================================f1
package aldwxstat.aldlink

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wangtaiyang on 2017/12/13.
  * 1
  */

object AdTrackingDetails {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)  //gcs:设定Log日志的显示的级别


    val spark = SparkSession.builder() //gcs:定义一个SparkSession
      .appName("AdTrackingDetails")
      .getOrCreate()

    println("创建全局管理表ald_link_trace")

    //==========================================================1
    /*gcs:
    *转 f2,1
    */
    JdbcUtil.readFromMysql(spark, "ald_link_trace").createGlobalTempView("ald_link_trace") //gcs:将MySql数据库中的ald_link_trace表中的内容全部读取出来，并且创建一个全局的临时表ald_link_trace


    //==========================================================2
    //gcs:全局的临时表的根数据库表是global_temp。这个操作是将全局的视图ald_link_trace 存放到cache当中
    spark.sqlContext.cacheTable("global_temp.ald_link_trace")


    execute(args, spark)

    spark.close()
  }


  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-7-11 <br>
  * <b>description:</b><br>
  * <b>param:</b><br>
  * <b>return:</b><br>
  */
  def execute(args: Array[String], spark: SparkSession): Unit = {

    //==========================================================3
    /*gcs:
    *转,f3,3
    */
    ArgsTool.analysisArgs(args)


    //==========================================================4
    /*
    *gcs:
    *对于args数据的分析
    */
    if (args.length == 0) {
      println("正常执行")
      //正常执行,从昨天开始算（包括昨天）
      fillDailyData(spark)
      //      fillSevenOrThirtyData(spark, "7")
      //      fillSevenOrThirtyData(spark, "30")
    } else if (ArgsTool.du != "" && ArgsTool.day != "") {
      println("补" + ArgsTool.du + ArgsTool.hour + "的数据")
      //补指定日期的7天或30天数据（包括指定的日期）
      fillSevenOrThirtyData(spark, ArgsTool.du)
    } else if (ArgsTool.du == "" && ArgsTool.day != "") {
      println("补" + ArgsTool.day + "的数据")
      //补某一天数据（包括指定的日期）
      fillDailyData(spark)
    }

  }



  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-3 <br>
  * <b>description:</b><br>
    *   将-d 那一天的数据读取出来。之后读取到今天的数据的操作。补每日数据 <br>
  * <b>param:</b><br>
    *   spark: SparkSession 传回来一个SparkSession对象 <br>
  * <b>return:</b><br>
    *   null <br>
  */
  def fillDailyData(spark: SparkSession): Unit = {
    //    val logs = ArgsTool.getDailyDataFrame(spark, DBConf.hdfsUrl)
    //==========================================================4
    /*gcs:
    *z,f3,4
    */
    val logs = ArgsTool.getDailyDataFrame(spark, ConfigurationUtil.getProperty("tongji.parquet"))

    //val logs = spark.read.parquet("/taiyang/link/20171214/etl-ald-log-2-2017121416")

    //==========================================================5
    //gcs:创建原始视图
    println("创建原始日志视图...")
    logs.createOrReplaceTempView("logs")


    //==========================================================7
    /*
    *gcs:
    *从log视图中读取数据，之后创建视图app_tmp
    */
    //每个指标求停留时长时需要用到这个表
    spark.sql("select ak,uu,at,dr,ifo,hour from logs where ev='app'").createOrReplaceTempView("app_tmp")
    spark.sqlContext.cacheTable("app_tmp")


    //==========================================================8
    /*
    *gcs:
    *将ak,uu字段之后，创建视图 nu_app_daily
    */
    //求出ev=app的ak，uu，每个指标算新用户时用到  ifo
    spark.sql("select ak,uu from app_tmp where ifo='true' group by ak,uu").createOrReplaceTempView("nu_app_daily")
    spark.sqlContext.cacheTable("nu_app_daily")


    //==========================================================9
    /*
    *gcs:
    *
    */
    spark.sql("select ak,uu,hour from app_tmp where ifo='true' group by ak,uu,hour").createOrReplaceTempView("nu_app_hourly")
    spark.sqlContext.cacheTable("nu_app_hourly")

    println("创建visitora_page...")

    //==========================================================6
    //gcs:使用filter语句，将形参当中的内容提取筛选出来
    logs.filter("ev='page' and ag_ald_link_key!='null' and ag_ald_position_id!='null' and ag_ald_media_id!='null' and scene!='null' ")
      .createOrReplaceTempView("visitora_page")

    spark.sql( //gcs:这里涉及到了一个case...when..end的使用方法
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
      """.stripMargin) //gcs:stripMargin 的含义是将字符串前面的“空格和|线”都去除掉
      .createOrReplaceTempView("visitora_page")
    spark.sqlContext.cacheTable("visitora_page")

    val linkDailyCollect: LinkAnalysis = new LinkDailyCollect(spark)
    val linkDailyDetails: LinkAnalysis = new LinkDailyDetails(spark)
    val mediaDailyDetails: LinkAnalysis = new MediaDailyDetails(spark)
    val positionDailyDeails: LinkAnalysis = new PositionDailyDeails(spark)
    val linkHourly: LinkAnalysis = new LinkHourlyDetails(spark)
    val mediaHourly: LinkAnalysis = new MediaHourlyDetails(spark)
    val positionHourly: LinkAnalysis = new PositionHourlyDetails(spark)

    val objAd = List[LinkAnalysis](linkDailyCollect, linkDailyDetails, mediaDailyDetails, positionDailyDeails, linkHourly,
      mediaHourly, positionHourly)
    objAd.foreach(x => {
      //每个指标调用执行
      adDailyData(x)
    })

    spark.sqlContext.uncacheTable("app_tmp") //gcs:将app_tmp 这个表从cache中清除掉
    spark.sqlContext.uncacheTable("nu_app_daily")
    spark.sqlContext.uncacheTable("nu_app_hourly")
    spark.sqlContext.uncacheTable("visitora_page")
  }

  def adDailyData(linkAnalysis: LinkAnalysis): Unit = {
    linkAnalysis.newUser()
    linkAnalysis.visitorCount()
    linkAnalysis.openCount()
    linkAnalysis.totalPageCount()
    linkAnalysis.totalStayTime()
    linkAnalysis.secondaryAvgStayTime()
    linkAnalysis.visitPageOnce()
    linkAnalysis.bounceRate()
    linkAnalysis.insert2db()
  }

  //补7天或30天数据
  /**<br>gcs:<br>
    * 7天或30天数据 <br>
    * @param spark 用于数据分析的sparkSession
    * @param numDays 要补充哪一天的数据。numDays 可能为7天，或者30天
    * */
  def fillSevenOrThirtyData(spark: SparkSession, numDays: String): Unit = {
    println(s"开始处理$numDays 天的数据")
    //val newSession=spark.newSession()
    new SingleAdLinkCollect().executeSingleSevenOrThirty(spark, numDays)
    new SevenAndThirtyDaysCollect().executeSevenOrDaysCollect(spark, numDays)

  }
}
