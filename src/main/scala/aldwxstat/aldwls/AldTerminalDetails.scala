//==========================================================
/*gcs:
*终端分析，当天的数据分析，每两小时运行一次
*/
package aldwxstat.aldwls

import java.sql.Timestamp

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangtaiyang on 2017/12/13.
  */

object AldTerminalDetails {
  def main(args: Array[String]): Unit = {//gcs:master

    Logger.getLogger("org").setLevel(Level.WARN) //gcs:显示日志的显示的级别


    val spark = SparkSession.builder()  //gcs:创建sparkSession
      .appName("AldTerminalDetails")
      .config("spark.sql.planner.skewJoin", "true")
      .config("spark.scheduler.mode", "FAIR")
      .getOrCreate()

    //==========================================================1
    /*
    *gcs:
    *计算各个维度下的指标
    */
    /*gcs:
    *“wvv" 客户端平台，例如：devtools(它是一个安卓平台下的系统)。
    *nt 网络类型 用户的手机当前连的是什么网络。这个网络可以是 数据流量，wifi
    *lang 用户在使用那种微信语言。zh_CN代表”中文“。你比如说我的微信的功能模块显示的都是英文，这就说明我选择的微信语言是”英文“。微信语言，例：zh_CN
    *wv 微信版本号 例6.5.6
    *sv 操作系统版本号 例iOS 10.0.1
    *wsdk 是客户端基础版本库。这个字段的含义是WebApp开发者使用了微信的哪个版本的SDK开发这个微信小程序。例：1.4.0
    *ww_wh 是？？？
    */
    //    将参数传入和sparksession传入，真正执行
    val terminal_sum = Array("wvv", "nt", "lang", "wv", "sv", "wsdk", "ww,wh")

    //==========================================================2
    /*
    *gcs:
    *将-d，-ak，之类的数据分析出来
    */
    ArgsTool.analysisArgs(args)  //gcs:分析传进来的参数

    //==========================================================3
    /*
    *gcs:
    *获得原始的log日志数据.因为这个getLogs方法中没有给numDays赋值。此时就会读取-du day 天的数据
    */
    val logs = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet")) //gcs:设定parquet的读取的目录


    logs.createOrReplaceTempView("logs")  //gcs:将获得的数据形成一个临时的视图logs


    //==========================================================4
    /*
    *gcs:
    *从logs当中筛选数据，并且创建视图。阅读出ev=app的字段的中的各个wvv,ak,uu等字段的信息
    */
    //gcs:从logs当中筛选数据，并且创建视图
    spark.sql("select ak,uu,at,wvv, nt, lang, wv, sv, wsdk, ww,wh,ifo,dr from logs where ev='app'")
      .createOrReplaceTempView("app_tmp")


    //==========================================================5
    /*
    *gcs:
    *将这个视图cache到内存当中
    */
    //spark.sql("select * from app_tmp").show()
    spark.sqlContext.cacheTable("app_tmp") //gcs:将创建的临时的视图，存储成cache当中 app_tmp
    //    获取需要在ev=page中计算的指标的字段

    //==========================================================6
    /*
    *gcs:
    *从ev=page当中将uu,at等这些信息读取出来创建 visitor_page 视图
    */
    spark.sql("select ak,uu,wvv, nt, lang, wv, sv, wsdk, ww,wh,pp,at from logs where ev='page'")
      .createOrReplaceTempView("visitor_page")

    spark.sqlContext.cacheTable("visitor_page")



    //==========================================================7
    /*
    *gcs:
    *terminal_sum 数组中存储着各个指标，循环这个指标，执行 execute(args, spark, tmp) 函数
    */
    for (tmp <- terminal_sum) execute(args, spark, tmp)


    //==========================================================8
    /*
    *gcs:
    *在这个程序运行完成之后，清除cache的缓存。
    * 同时执行close这个函数。关闭这个sparkSession
    */
    spark.catalog.clearCache()
    spark.close()
  }

  /**
    *
    */
  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-8 <br>
  * <b>description:</b><br>
    *   判断执行昨日数据还是补某一天的数据<br>
    *   当args.length 为0的时候，此时就正常执行从昨天开始的数据。当srgs.length不为0的时候，就
  * <b>param:</b><br>
    *   args: Array[String] ;当args.length == 0 此时说明提交jar包的用户没有提交程序，此时就会从昨天开始执行数据 <br>
    *     spark: SparkSession ；从SparkSession当中提取数据 <br>
    *       tmp: String ;tmp是什么呢？？？？ <br>
  * <b>return:</b><br>
    *   null <br>
  */
  def execute(args: Array[String], spark: SparkSession, tmp: String): Unit = {
    //ArgsTool.analysisArgs(args)  ---
    if (args.length == 0) {
      println("正常执行")
      //正常执行,从昨天开始算（包括昨天）
      fillDailyData(args, spark, tmp)
    } else if (ArgsTool.du == "" && ArgsTool.day != "") {
      println("补" + ArgsTool.day + "的数据")
      fillDailyData(args, spark, tmp)
    }
  }



  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-5-8 <br>
  * <b>description:</b><br>
    *   真正执行昨天或某一天数据 <br>
  * <b>param:</b><br>
    *   args: Array[String] ;
    *   spark: SparkSession ;
    *   allDataEveryValue: String ;
  * <b>return:</b><br>
  */
  def fillDailyData(args: Array[String], spark: SparkSession, allDataEveryValue: String): Unit = {
    /**
      * 数据源分三种：app（小程序）、page（页面）、event（时间），只有分享取ev=event
      * 获取需要在ev=app中计算的指标的字段
      */

    // 创建对象，将sparksession和需要执行的维度出进去
    //==========================================================1
    /*
    *gcs:
    *创建一个PublicModularDetails对象，将sparksession和需要执行的维度出进去 <br>
    *spark 就是要用来执行的sparkSession
    *allDataEveryValue 就是要用来计算的维度的
    */
    val terminalDailyDetails: PublicModularDetails = new PublicModularDetails(spark, allDataEveryValue)

    //==========================================================2
    /*
    *gcs:
    *开始运行程序
    */
    //调用最终执行的接口函数
    adDailyData(terminalDailyDetails)
    insert2db(spark, allDataEveryValue)
  }


  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-7-16 <br>
  * <b>description:</b><br>
    *计算各个新用户数、访问人数、打开次数...指标
  * <b>param:</b><br>
  * <b>return:</b><br>
  */
  def adDailyData(terminalAnalysis: PublicModularDetails): Unit = {
    terminalAnalysis.newUser()
    terminalAnalysis.visitorCount()
    terminalAnalysis.openCount()
    terminalAnalysis.totalPageCount()
    terminalAnalysis.totalStayTime()
    terminalAnalysis.secondaryAvgStayTime()
    terminalAnalysis.visitPageOnce()
    terminalAnalysis.bounceRate()
  }


  /**
  * <b>author:</b> gcs <br>
  * <b>data:</b> 18-7-16 <br>
  * <b>description:</b><br>
    *   将上面计算完成的各个指标的数据，插入到数据库中
  * <b>param:</b><br>
  * <b>return:</b><br>
  */
  def insert2db(spark: SparkSession, typeValue: String): Unit = {
    val day = ArgsTool.day
    val UpDataTime = new Timestamp(System.currentTimeMillis())

    //==========================================================1
    /*
    *gcs:
    *从每一个指标的视图中，将同一个ak和uu下的指标都聚合在一块儿，之后插入到MYSql数据库
    */
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

    //result.show()

    result.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      // r(0) 小程序唯一标识,r(1) 二维码组唯一标识,r(2) 扫码人数,r(3) 扫码次数,r(4) 扫码带来新增
      val sqlText = s"insert into aldstat_terminal_analysis (app_key,day,new_comer_count, visitor_count,open_count,total_page_count,avg_stay_time,total_stay_time,bounce_rate,one_page_count,update_at,type,type_value)" +
        s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE " +
        s"new_comer_count=?, visitor_count=?, open_count=?,total_page_count=?, avg_stay_time=?,total_stay_time=?, bounce_rate=?,one_page_count=?,update_at=?,type_value=?"


      //conn.setAutoCommit(false)
      rows.foreach(r => {
        //val day = dataTime_tmp
        val update_at = UpDataTime
        val app_key = r.get(0)
        val type_values = r.get(1)
        val total_page_count = r.get(2)
        val visitor_count = r.get(3)
        val open_count = r.get(4)
        val total_stay_time = r.get(5)
        val avg_stay_time = r.get(6)
        val new_comer_count = r.get(7)
        val one_page_count = r.get(8)
        val bounce_rate = r.get(9)
        val ty = typeValue

        params.+=(Array[Any](app_key, day, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, total_stay_time, bounce_rate, one_page_count, update_at, ty, type_values,
          new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, total_stay_time, bounce_rate, one_page_count, update_at, type_values))

      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })
  }
}
