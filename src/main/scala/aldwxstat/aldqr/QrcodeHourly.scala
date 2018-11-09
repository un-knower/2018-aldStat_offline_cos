package aldwxstat.aldqr

import aldwxutils.{ArgsTool, JdbcUtil, TimeUtil}
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.mutable.ArrayBuffer

class QrcodeHourly(spark: SparkSession) extends QrocdeAnalysis {

  /**
    * 二维码每小时的扫码人数和扫码次数
    *
    * @param df
    * @return
    */
  def qr_visitor_and_scan_count(df: DataFrame): DataFrame = {
    val page_df = df.filter("ev='page' and ag_aldsrc !='null' and qr !='null'")
      .select(
        df("ak").alias("app_key"),
        df("qr").alias("qr_key"),
        df("hour"),
        df("at"),
        df("uu")
      )

    val qr_uv_df = page_df.groupBy("app_key", "qr_key", "hour")
      .agg(
        countDistinct("uu") as "qr_visitor_count",
        countDistinct("at") as "qr_scan_count"
      )
    qr_uv_df
  }

  /**
    * 二维码每小时的扫码带来新增
    *
    * @param df DataFrame
    * @return DataFrame
    */
  def qr_newer_count(df: DataFrame): DataFrame = {
    // 每天访问小程序的新人数
    val app_df = df.filter("ev='app' and ifo='true'")
      .select(
        df("ak").alias("app_key"),
        df("hour"),
        df("uu"),
        df("at")
      )

    // 二维码数据源
    val page_df = df.filter("ev='page' and ag_aldsrc !='null' and qr !='null'")
      .select(
        df("ak").alias("app_key"),
        df("qr").alias("qr_key"),
        df("hour"),
        df("at"),
        df("uu")
      )

    // 每天扫码带来新增
    val qr_newer_df = page_df.join(
      app_df,
      page_df("app_key") === app_df("app_key") &&
        page_df("hour") === app_df("hour") &&
        page_df("at") === app_df("at") &&
        page_df("uu") === app_df("uu")
    ).select(
      page_df("app_key"),
      page_df("hour"),
      page_df("qr_key"),
      app_df("uu")
    ).groupBy("app_key", "qr_key", "hour")
      .agg(
        countDistinct("uu").alias("qr_newer_count")
      )
    qr_newer_df
  }

  /**
    * 每小时扫码人数,扫码次数入库
    *
    * @param df 读取某一天的 parquet 文件形成的 DataFrame
    */
  def insert2db(df: DataFrame): Unit = {
    val the_day = ArgsTool.day // 日期
    // 生成结果表
    val qr_uv_df: DataFrame = qr_visitor_and_scan_count(df)
    val qr_newer_df: DataFrame = qr_newer_count(df)

    val _result_df = qr_uv_df.join(
      qr_newer_df,
      qr_uv_df("app_key") === qr_newer_df("app_key") &&
        qr_uv_df("hour") === qr_newer_df("hour") &&
        qr_uv_df("qr_key") === qr_newer_df("qr_key"),
      "leftouter"
    ).select(
      qr_uv_df("app_key"),
      qr_uv_df("hour"),
      qr_uv_df("qr_key"),
      qr_uv_df("qr_visitor_count"),
      qr_uv_df("qr_scan_count"),
      qr_newer_df("qr_newer_count")
    ).na.fill(0)

    // 逐行入库
    _result_df.foreachPartition((rows: Iterator[Row]) => {
      val update_at = TimeUtil.nowInt() // 数据更新时间
//      val conn = JdbcUtil.getConn()
//      val statement = conn.createStatement
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText = s"insert into aldstat_hourly_qr (app_key,day,hour,qr_key,qr_visitor_count," +
        s"qr_scan_count,qr_newer_count)" +
        s"values (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE qr_visitor_count=?, qr_scan_count=?, qr_newer_count=?"

      rows.foreach(r => {
        val app_key = r.get(0) // 小程序唯一标识
        val hour = r.get(1) // 小时
        val qr_key = r.get(2) // 二维码唯一标识
        val qr_visitor_count = r.get(3) // 扫码人数
        val qr_scan_count = r.get(4) // 扫码次数
        val qr_newer_count = r.get(5) // 扫码带来新增

        params.+=(Array[Any](app_key, the_day, hour, qr_key, qr_visitor_count, qr_scan_count, qr_newer_count, qr_visitor_count, qr_scan_count, qr_newer_count))
      })
      // try {
      JdbcUtil.doBatch(sqlText, params) // 批量入库
      //      }
      //      finally{
      //        statement.close() // 关闭 statement
      //        conn.close()      // 关闭数据库连接
      //      }
    })
  }
}
