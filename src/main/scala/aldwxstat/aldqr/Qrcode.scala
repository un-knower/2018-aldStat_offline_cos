package aldwxstat.aldqr


import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, AldwxDebug}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Qrcode {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    // 打印调试信息
    AldwxDebug.debug_info("2017-11-01", "sunxiaowei")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .getOrCreate()

    execute(args, spark)
    spark.stop()
  }

  def execute(args: Array[String], spark: SparkSession): Unit = {
    ArgsTool.analysisArgs(args)

    if (args.length == 0) {
      println("正常执行")
      //正常执行,从昨天开始算（包括昨天）
      qrDaily(args, spark)
    }

    if (ArgsTool.du == "" && ArgsTool.day != "") {
      println("补" + ArgsTool.day + "的数据")
      //补某一天数据（包括指定的日期）
      qrDaily(args, spark)
    }
  }

  def qrDaily(args: Array[String], spark: SparkSession): Unit = {
    // 读取某天的 parquet 文件为 dataframe
//    val df = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet"))
    ArgsTool.analysisArgs(args)
    val du = ArgsTool.du
    val df =ArgsTool.getTencentSevenOrThirty(spark,du)
//    val df =ArgsTool.getTencentDailyDataFrame(spark, ConfigurationUtil.getProperty("tencent.parquet"))

    val day = ArgsTool.day
    val qrcodeDaily: QrocdeAnalysis = new QrcodeDaily(spark)
    val qrcodeHourly: QrocdeAnalysis = new QrcodeHourly(spark)
    val qrcodeGroupDaily: QrocdeAnalysis = new QrcodeGroupDaily(spark)
    val qrcodeGroupHourly: QrocdeAnalysis = new QrcodeGroupHourly(spark)

    qrcodeDaily.insert2db(df) // 二维码每天扫码人数,扫码次数,扫码带来新增入库
    qrcodeHourly.insert2db(df) // 二维码每小时扫码人数,扫码次数,扫码带来新增入库
    qrcodeGroupDaily.insert2db(df) // 二维码组每天扫码人数,扫码次数,扫码带来新增入库
    qrcodeGroupHourly.insert2db(df) // 二维码组每小时扫码人数,扫码次数,扫码带来新增入库
  }
}

