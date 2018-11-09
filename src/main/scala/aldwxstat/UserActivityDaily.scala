package aldwxstat

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil, TimeUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaoxiang on 2017/8/22.
  * 用户活跃度
  */
object UserActivityDaily {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val yesterday = TimeUtil.processArgs(args)

    //创建sparksession
    val spark = SparkSession.builder().appName(this.getClass.getName)
      .getOrCreate()


    execute(args, spark)


    //最终表所需要的数据
    val result = spark.sql("SELECT t3.app_key," + yesterday + " day, t2.dau,t1.wau, " +
      "(cast(t2.dau as double) / cast(t1.wau as double)) AS dau_wau_ratio ," +
      "t3.mau ," +
      "(cast(t2.dau as double) / cast(t3.mau as double)) AS dau_mau_ratio , " +
      "NOW() as update_at FROM t3 LEFT JOIN t1 ON t3.app_key =t1.app_key LEFT JOIN " +
      "t2 ON t3.app_key =t2.app_key where length(t3.app_key)=32").na.fill(0)

    result.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = s"insert into aldstat_user_activity (app_key,day,dau,wau, dau_wau_ratio," +
        s" mau,dau_mau_ratio,update_at) " +
        s" values (?,?,?,?,?,?,?,?) ON " +
        s" DUPLICATE KEY UPDATE dau=?, wau=?, dau_wau_ratio=?,mau=?,dau_mau_ratio=?"

      rows.foreach(r => {
        val app_key = r.get(0)
        val day = r.get(1)
        val dau = r.get(2)
        val wau = r.get(3)
        val dau_wau_ratio = r.get(4)
        val mau = r.get(5)
        val dau_mau_ratio = r.get(6)
        val update_at = r.get(7)

        params.+=(Array[Any](app_key, day, dau, wau, dau_wau_ratio, mau, dau_mau_ratio, update_at, dau, wau, dau_wau_ratio, mau, dau_mau_ratio))

      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })
    //关闭资源
    spark.close()

  }

  def execute(args: Array[String], spark: SparkSession): Unit = {
    ArgsTool.analysisArgs(args)
    println("正常执行")
    //正常执行,从昨天开始算（包括昨天）
    fillDailyData(spark)
    sevenData(spark, "7")
    thirtyData(spark, "30")
  }

  //日活跃
  def fillDailyData(spark: SparkSession): Unit = {
    //    val logs = ArgsTool.getDailyDataFrame(spark, DBConf.hdfsUrl)
//    val logs = ArgsTool.getDailyDataFrame(spark, ConfigurationUtil.getProperty("tongji.parquet"))
    val logs =ArgsTool.getTencentDailyDataFrame(spark, ConfigurationUtil.getProperty("tencent.parquet"))
    //val logs = spark.read.parquet("/taiyang/link/20171214/etl-ald-log-2-2017121416")

    println("创建原始日志视图...")
    logs.createOrReplaceTempView("logs")

    //每个指标求停留时长时需要用到这个表
    spark.sql("select ak,uu,ev from logs where ev='app'").createOrReplaceTempView("app_tmp")

    spark.sql("SELECT ak app_key,COUNT(DISTINCT uu) dau FROM app_tmp WHERE ev = 'app' GROUP BY ak ")
      .createTempView("t2")
  }

  //周活跃
  def sevenData(spark: SparkSession, numDays: String): Unit = {
    println(s"开始处理$numDays 天的数据")
//    val logs = ArgsTool.getSevenOrThirtyDF(spark, ConfigurationUtil.getProperty("tongji.parquet"), numDays)
    val logs =ArgsTool.getTencentSevenOrThirty(spark,numDays)
    logs.createTempView("t_wau")
    spark.sql("SELECT ak app_key,COUNT(DISTINCT uu) wau FROM t_wau WHERE ev = 'app' GROUP BY ak ")
      .createTempView("t1")

  }

  //月活跃读取数据
  def thirtyData(spark: SparkSession, numDays: String): Unit = {
    println(s"开始处理$numDays 天的数据")
//    val logs = ArgsTool.getSevenOrThirtyDF(spark, ConfigurationUtil.getProperty("tongji.parquet"), numDays)
val logs =ArgsTool.getTencentSevenOrThirty(spark,numDays)
    logs.createTempView("t_mau")

    spark.sql("SELECT ak app_key,COUNT(DISTINCT uu) mau FROM t_mau WHERE ev = 'app' GROUP BY ak ")
      .createTempView("t3")
  }
}
