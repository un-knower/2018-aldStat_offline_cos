package aldwxstat.aldwls

import java.sql.Timestamp
import java.util.Properties

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangtaiyang on 2017/12/13.
  */

object AldPhoneModelDetails {
  //  需要在group by 后面分组的（正常情况下就是不同的维度）
  val dimensionAnalysis = "model"

  /**
    * 主函数
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("AldPhoneModelDetails")
      //      .master("local")
      //      .config("spark.sql.shuffle.partitions", 12)
      .getOrCreate()
    //    val url = DBConf.url
    //    val prop = new Properties()
    //
    //    prop.put("driver", DBConf.driver)
    //    prop.setProperty("user", DBConf.user)
    //    prop.setProperty("password", DBConf.password)
    val url = ConfigurationUtil.getProperty("jdbc.url")
    val prop = new Properties()
    prop.put("driver", ConfigurationUtil.getProperty("jdbc.driver"))
    prop.setProperty("user", ConfigurationUtil.getProperty("jdbc.user"))
    prop.setProperty("password", ConfigurationUtil.getProperty("jdbc.pwd"))
    //新用户数+访问人数+访问次数+打开次数+次均停留时长+跳出率
    spark.read.jdbc(url, "phone_model", prop).createOrReplaceTempView("phone_model")
    //
    //    DBConf.read_from_mysql(spark, "phone_model").createGlobalTempView("phone_model")
    //    spark.sqlContext.cacheTable("phone_model")
    spark.sql("select * from phone_model").show()
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

    //    val logs = Aldstat_args_tool.analyze_args(args,spark, DBConf.hdfsUrl)
    val logs = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet"))
    println("创建原始日志视图...")
    logs.createOrReplaceTempView("logs")

    val phoneModelTable = spark.sql(s"SELECT a.ak,a.uu,a.dr,a.ifo,a.ev,a.at,a.pp,a.st,b.name ${dimensionAnalysis} FROM logs a left join phone_model b on a.pm = b.uname")
    phoneModelTable.createOrReplaceTempView("phone_model_data_all")
    //数据源分三种：app（小程序）、page（页面）、event（时间），只有分享取ev=event
    //    获取需要在ev=app中计算的指标的字段
    spark.sql(s"select ak,uu,at,${dimensionAnalysis},ifo,dr from phone_model_data_all where ev='app'").createOrReplaceTempView("app_tmp")
    spark.sqlContext.cacheTable("app_tmp")
    //    获取需要在ev=page中计算的指标的字段
    spark.sql(s"select ak,uu,${dimensionAnalysis},pp,at from phone_model_data_all where ev='page'")
      .createOrReplaceTempView("visitor_page")

    spark.sqlContext.cacheTable("visitor_page")

    //    创建对象，将sparksession和需要执行的维度出进去province
    val modelDailyDetails: PublicModularDetails = new PublicModularDetails(spark, dimensionAnalysis)
    //    调用最终执行的接口函数
    adDailyData(modelDailyDetails)

    insert2db(spark)
    //    清除缓存表
    spark.sqlContext.uncacheTable("app_tmp")
    spark.sqlContext.uncacheTable("visitor_page")
  }

  /**
    * 对创建的类进行实现
    *
    * @param modelAnalysis
    */
  def adDailyData(modelAnalysis: PublicModularDetails): Unit = {
    modelAnalysis.newUser()
    modelAnalysis.visitorCount()
    modelAnalysis.openCount()
    modelAnalysis.totalPageCount()
    modelAnalysis.totalStayTime()
    modelAnalysis.secondaryAvgStayTime()
    modelAnalysis.visitPageOnce()
    modelAnalysis.bounceRate()
  }

  /**
    * 在上面所有指标都用行完之后将其数据入库
    *
    * @param spark
    */
  def insert2db(spark: SparkSession): Unit = {
    val day = aldwxutils.TimeUtil.StrToLong(ArgsTool.day)
    //    print("aaaa"+day)
    val UpDataTime = new Timestamp(System.currentTimeMillis())
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
    println(day)
    println(ArgsTool.day)
    result.show()
    result.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      // r(0) 小程序唯一标识,r(1) 二维码组唯一标识,r(2) 扫码人数,r(3) 扫码次数,r(4) 扫码带来新增
      val sqlText = s"insert into ald_device_statistics (app_key,date,new_user_count, visitor_count,open_count,page_count,secondary_stay_time,total_stay_time,bounce_rate,one_page_count,update_at,phone_model)" +
        s"values (?,?,?,?,?,?,?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE new_user_count=?, visitor_count=?, open_count=?,page_count=?, secondary_stay_time=?,total_stay_time=?, bounce_rate=?,one_page_count=?,update_at=?"

      //conn.setAutoCommit(false)
      rows.foreach(r => {
        //val date = ArgsTool.day//自己修改的
        val date = day
        val update_at = UpDataTime
        val app_key = r.get(0)
        var type_values = ""
        val page_count = r.get(2)
        val visitor_count = r.get(3)
        val open_count = r.get(4)
        val total_stay_time = r.get(5)
        val secondary_stay_time = r.get(6)
        val new_user_count = r(7)
        val one_page_count = r.get(8)
        val bounce_rate = r.get(9)

        if (r(1) == "" || r(1) == "null" || r(1) == null || r(1) == "undefined") {
          type_values = "未知"
        } else {
          type_values = r(1).toString()
        }
        params.+=(Array[Any](app_key, date, new_user_count, visitor_count, open_count, page_count, secondary_stay_time, total_stay_time, bounce_rate, one_page_count, update_at, type_values,
          new_user_count, visitor_count, open_count, page_count, secondary_stay_time, total_stay_time, bounce_rate, one_page_count, update_at))

      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })
  }
}
