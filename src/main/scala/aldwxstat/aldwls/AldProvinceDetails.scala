package aldwxstat.aldwls

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangtaiyang on 2017/12/13.
  */

object AldProvinceDetails {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("AldProvinceDetails")
      //      .master("local")
      //      .config("spark.sql.shuffle.partitions", 12)
      .getOrCreate()
    //    将参数传入和sparksession传入，真正执行
    execute(args, spark)
    spark.close()
  }

  /**
    * 判断执行昨日数据还是某一天的数据
    *
    * @param args
    * @param spark
    */
  def execute(args: Array[String], spark: SparkSession): Unit = {
    ArgsTool.analysisArgs(args)
    if (args.length == 0) {
      println("正常执行")
      //正常执行,从昨天开始算（包括昨天）
      fillDailyData(args, spark)
    } else if (ArgsTool.du == "" && ArgsTool.day != "") {
      println("补" + ArgsTool.day + "的数据")
      fillDailyData(args, spark)
    }
  }


  //真正执行昨天或某一天数据
  def fillDailyData(args: Array[String], spark: SparkSession): Unit = {
    val logs = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet"))

    println("创建原始日志视图...")
    logs.createOrReplaceTempView("logs")

    //数据源分三种：app（小程序）、page（页面）、event（时间），只有分享取ev=event
    //    获取需要在ev=app中计算的指标的字段
    spark.sql("select ak,uu,at,province,ifo,dr from logs where ev='app'").createOrReplaceTempView("app_tmp")
    spark.sqlContext.cacheTable("app_tmp")
    //    获取需要在ev=page中计算的指标的字段
    spark.sql("select ak,uu,province,pp,at from logs where ev='page'")
      .createOrReplaceTempView("visitor_page")

    spark.sqlContext.cacheTable("visitor_page")

    //    创建对象，将sparksession和需要执行的维度出进去province
    val provinceDailyDetails: PublicModularDetails = new PublicModularDetails(spark, "province")
    //    调用最终执行的接口函数
    adDailyData(provinceDailyDetails)
    insert2db(spark)
    //    清除缓存表
    spark.sqlContext.uncacheTable("app_tmp")
    spark.sqlContext.uncacheTable("visitor_page")
  }

  /**
    * 对创建的类进行实现
    *
    * @param provinceAnalysis
    */
  def adDailyData(provinceAnalysis: PublicModularDetails): Unit = {
    provinceAnalysis.newUser()
    provinceAnalysis.visitorCount()
    provinceAnalysis.openCount()
    provinceAnalysis.totalPageCount()
    provinceAnalysis.totalStayTime()
    provinceAnalysis.secondaryAvgStayTime()
    provinceAnalysis.visitPageOnce()
    provinceAnalysis.bounceRate()
  }

  /**
    * 在上面所有指标都用行完之后将其数据入库
    *
    * @param spark
    */
  def insert2db(spark: SparkSession): Unit = {
    val day = aldwxutils.TimeUtil.StrToLong(ArgsTool.day)
    //    print(day)
    val UpDataTime = (System.currentTimeMillis() / 1000).toInt
    val result = spark.sql(
      """
        |select tpc.app_key,tpc.tmp_sum,tpc.total_page_count,vc.visitor_count,oc.open_count,
        |tst.total_stay_time,sast.secondary_avg_stay_time,nu.new_comer_count,vpo.one_page_count,bounce.bounce_rate
        |from total_page_count tpc
        |left join visitor_count vc
        |on vc.app_key=tpc.app_key and vc.tmp_sum=tpc.tmp_sum
        |left join open_count oc
        |on tpc.app_key=oc.app_key and tpc.tmp_sum=oc.tmp_sum
        |left join total_stay_time tst
        |on tpc.app_key=tst.app_key and tpc.tmp_sum=tst.tmp_sum
        |left join secondary_avg_stay_time sast
        |on tpc.app_key=sast.app_key and tpc.tmp_sum=sast.tmp_sum
        |left join new_user_daily nu
        |on tpc.app_key=nu.app_key and tpc.tmp_sum=nu.tmp_sum
        |left join visit_page_once vpo
        |on tpc.app_key=vpo.app_key and tpc.tmp_sum=vpo.tmp_sum
        |left join bounce_rate bounce
        |on tpc.app_key=bounce.app_key and tpc.tmp_sum=bounce.tmp_sum
      """.stripMargin).filter("tmp_sum != ''").na.fill(0)
    result.show()
    result.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      // r(0) 小程序唯一标识,r(1) 二维码组唯一标识,r(2) 扫码人数,r(3) 扫码次数,r(4) 扫码带来新增
      val sqlText = s"insert into aldstat_region_statistics (app_key,day,new_user_count, visitor_count,open_count,page_count,secondary_stay_time,total_stay_time,bounce_rate,one_page_count,update_at,province)" +
        s"values (?,?,?,?,?,?,?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE " +
        s"new_user_count=?, visitor_count=?, open_count=?,page_count=?, secondary_stay_time=?,total_stay_time=?, bounce_rate=?,one_page_count=?,update_at=?"


      //conn.setAutoCommit(false)
      rows.foreach(r => {
        //        val day = dataTime_tmp
        //        val date = ArgsTool.day
        val update_at = UpDataTime
        val app_key = r.get(0)
        val type_values = r.get(1)
        val page_count = r.get(2)
        val visitor_count = r.get(3)
        val open_count = r.get(4)
        val total_stay_time = r.get(5)
        val secondary_stay_time = r.get(6)
        val new_user_count = r.get(7)
        val one_page_count = r.get(8)
        val bounce_rate = r.get(9)
        params.+=(Array[Any](app_key, day, new_user_count, visitor_count, open_count, page_count, secondary_stay_time, total_stay_time, bounce_rate, one_page_count, update_at, type_values, new_user_count, visitor_count, open_count, page_count, secondary_stay_time, total_stay_time, bounce_rate, one_page_count, update_at))

      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })
  }
}
