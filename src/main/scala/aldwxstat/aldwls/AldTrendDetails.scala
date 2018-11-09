package aldwxstat.aldwls

import aldwxconfig.ConfigurationUtil
import aldwxutils.ArgsTool
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wangtaiyang on 2017/12/13.
  */

/*
*gcs:
*这是趋势分析的模块的代码
* 在补数据的时候直接指定-d 的执行时间就可以了。只需要指定了-d的运行参数，就可以同时将day和hour的数据分析出来
*/
object AldTrendDetails {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("AldTrendDetails")
      //      .master("local")
      //.config("spark.sql.shuffle.partitions", 12)
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
    //==========================================================1
    /*
    *gcs:
    *将程序参数中的-du,-d,-hour 等参数读取出来，并且为这些参数进行赋值
    */
    ArgsTool.analysisArgs(args)
    if (args.length == 0) { //gcs:如果在运行趋势分析的任务的时候args的参数的个数是0的话，此时就会正常的执行这些数据
      println("正常执行")
      //正常执行,从昨天开始算（包括昨天）
      fillDailyData(args, spark)
    } else if (ArgsTool.du == "" && ArgsTool.day != "") {
      println("补" + ArgsTool.day + "的数据")
      fillDailyData(args, spark)
    }
  }


  //
  /**<br>gcs:<br>
    * 真正执行昨天或某一天数据
    * */
  def fillDailyData(args: Array[String], spark: SparkSession): Unit = {
//    val logs = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet"))
    //==========================================================2
    /*
    *gcs:
    *获取-d 那天开始的 -du天的数据
    */
    val logs =ArgsTool.getTencentDailyDataFrame(spark, ConfigurationUtil.getProperty("tencent.parquet"))

    //==========================================================3
    /*
    *gcs:
    *将读取出来的数据设定为logs视图
    */
    println("创建原始日志视图...")
    logs.createOrReplaceTempView("logs")

    //数据源分三种：app（小程序）、page（页面）、event（时间），只有分享取ev=event
    //    获取需要在ev=app中计算的指标的字段
    //==========================================================4
    /*
    *gcs:
    *这是将从ev=app 类型中读取数据，创建了app_tmp类型的数据
    */
    spark.sql("select ak,uu,at,ifo,dr,hour from logs where ev='app' and hour !='null'").createOrReplaceTempView("app_tmp")
    spark.sqlContext.cacheTable("app_tmp")
    //    获取需要在ev=page中计算的指标的字段

    //==========================================================5
    /*
    *gcs:
    *从ev=page字段中将page字段的数据读取出来，留作后边的各个指标的分析的
    */
    spark.sql("select ak,uu,pp,at,hour from logs where ev='page' and hour !='null'")
      .createOrReplaceTempView("visitor_page")

    spark.sql("select ak,path,ct,tp,hour from logs where ev='event' and hour !='null'")
      .createOrReplaceTempView("visitor_event")

    spark.sqlContext.cacheTable("visitor_page")

    //==========================================================6
    /*
    *gcs:
    *这是创建两个指标类，trend的TrendModularDetails ,用于当天的数据的分析。
    * hour 类
    */
    //    创建对象，将sparksession和需要执行的维度出进去
    val trendDailyDetails: TrendModularDetails = new TrendModularDetails(spark, "trend")

    //==========================================================13
    /*
    *gcs:
    *当我在指定了使用一天的数据进行分析时，虽然只是指定了-d 的参数，但是同样还是会执行hour的数据。原因是我们上报的日志当中已经有hour这个字段了，
    * 它会将原始数据中的hour字段进行分析，所有在aldTrendDetails 当中对程序进行分析完成之后，会对小时的数据进行分析
    */
    val hourDailyDetails: TrendModularDetails = new TrendModularDetails(spark, "hour")
    //    调用最终执行的接口函数
    //==========================================================7
    /*
    *gcs:
    *调用TrendModularDetails的类的对象来计算出各个类的指标
    */
    adDailyData(trendDailyDetails)
    //==========================================================8
    /*
    *gcs:
    *将最终的结果插入到MySql当中去。这是插入的今天的数据
    */
    new PublicMysqlInsertDetails(spark).trendInsert2db()

    //==========================================================9
    /*
    *gcs:
    *将最终的结果插入到MySql当中去，这是插入的小时的数据
    */
    adDailyData(hourDailyDetails)
    //==========================================================10
    /*
    *gcs:
    *将最终的数每小时的据插入到了MySql当中
    */
    new PublicMysqlInsertDetails(spark).hourTrendInsert2db()
    //    清除缓存表
    spark.sqlContext.uncacheTable("app_tmp")
    spark.sqlContext.uncacheTable("visitor_page")
  }

  /**
    * 对创建的类进行实现
    *
    * @param trendAnalysis
    */
  def adDailyData(trendAnalysis: TrendModularDetails): Unit = {
    trendAnalysis.newUser() //gcs:计算新的用户数
    trendAnalysis.visitorCount() //gcs:计算访问人数
    trendAnalysis.openCount() //gcs:计算打开次数
    trendAnalysis.totalPageCount() //gcs:计算总的页面的次数
    trendAnalysis.totalStayTime() //gcs:计算总的停留的时间
    trendAnalysis.secondaryAvgStayTime()  //gcs:计算平均的停留时间
    trendAnalysis.visitPageOnce() //gcs:计算访问一次的人数
    trendAnalysis.bounceRate() //gcs:计算跳出率
    trendAnalysis.shareCount() //gcs:计算页面的访问的次数
  }
}
