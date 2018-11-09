package aldwxstat.aldqr

import aldwxutils.{ArgsTool, JdbcUtil, TimeUtil}
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

class QrcodeGroupHourly(spark: SparkSession) extends QrocdeAnalysis {

  /**
    * 二维码组每小时扫码人数和扫码次数
    *
    * @param df DataFrame
    * @return DataFrame
    */
  def qr_visitor_and_scan_count(df: DataFrame): DataFrame = {
    val qr_info_df = JdbcUtil.readFromMysql(spark, "(select app_key,qr_key,qr_group_key from ald_code) as code_df")

    // 二维码数据源
    val page_df = df.filter("ev='page' and ag_aldsrc !='null' and qr !='null'")
      .select(
        df("ak").alias("app_key"),
        df("qr").alias("qr_key"),
        df("hour"),
        df("at"),
        df("uu")
      )

    // 二维码组每小时的扫码人数,扫码次数
    val qr_group_uv_df = qr_info_df.join(
      page_df,
      qr_info_df("app_key") === page_df("app_key") &&
        qr_info_df("qr_key") === page_df("qr_key")
    ).select(
      qr_info_df("app_key"),
      qr_info_df("qr_group_key"),
      page_df("hour"),
      page_df("at"),
      page_df("uu")
    ).groupBy("app_key", "qr_group_key", "hour")
      .agg(
        countDistinct("uu") as "qr_visitor_count",
        countDistinct("at") as "qr_scan_count"
      )
    qr_group_uv_df
  }

  /**
    * 二维码组每小时扫码带来新增
    *
    * @param df DataFrame
    * @return DataFrame
    */
  def qr_newer_count(df: DataFrame): DataFrame = {
    val qr_info_df = JdbcUtil.readFromMysql(spark, "(select app_key,qr_key,qr_group_key from ald_code) as code_df")

    // 二维码数据源
    val page_df = df.filter("ev='page' and ag_aldsrc !='null' and qr !='null'")
      .select(
        df("ak").alias("app_key"),
        df("qr").alias("qr_key"),
        df("hour"),
        df("at"),
        df("uu")
      )

    // 新访问用户
    val ifo_df = df.filter("ev='app' and ifo='true'")
      .select(
        df("ak").alias("app_key"),
        df("hour"),
        df("at"),
        df("uu")
      )

    // 每个小时的新访问用户
    val qr_newer_df = page_df.join(
      ifo_df,
      page_df("app_key") === ifo_df("app_key") &&
        page_df("hour") === ifo_df("hour") &&
        page_df("at") === ifo_df("at") &&
        page_df("uu") === ifo_df("uu")
    ).select(
      page_df("app_key"),
      page_df("qr_key"),
      page_df("hour"),
      page_df("at"),
      ifo_df("uu")
    )

    // 二维码组每小时的扫码带来新增
    val qr_group_newer_df = qr_newer_df.join(
      qr_info_df,
      qr_info_df("app_key") === qr_newer_df("app_key") &&
        qr_info_df("qr_key") === qr_newer_df("qr_key"),
      "leftouter"
    ).select(
      qr_newer_df("app_key"),
      qr_newer_df("hour"),
      qr_newer_df("uu"),
      qr_info_df("qr_group_key")
    ).groupBy("app_key", "hour", "qr_group_key")
      .agg(
        countDistinct("uu").alias("qr_newer_count")
      )

    qr_group_newer_df
  }

  /**
    * 二维码组每小时扫码人数,扫码次数,扫码带来新增的入库
    *
    * @param df
    */
  def insert2db(df: DataFrame): Unit = {
    val qr_uv_df: DataFrame = qr_visitor_and_scan_count(df)
    val qr_newer_df: DataFrame = qr_newer_count(df)
    val the_day = ArgsTool.day // 日期

    // 形成结果表
    val _result_df = qr_uv_df.join(
      qr_newer_df,
      qr_uv_df("app_key") === qr_newer_df("app_key") &&
        qr_uv_df("qr_group_key") === qr_newer_df("qr_group_key") &&
        qr_uv_df("hour") === qr_newer_df("hour")
    ).select(
      qr_uv_df("app_key"),
      qr_uv_df("hour"),
      qr_uv_df("qr_group_key"),
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
      val sqlText = s"insert into aldstat_hourly_qr_group (app_key,day,hour,qr_group_key,qr_visitor_count,qr_scan_count,qr_newer_count)" +
        s"values (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE qr_visitor_count=?, qr_scan_count=?, qr_newer_count=?"

      rows.foreach(r => {
        val (app_key, hour, qr_group_key, qr_visitor_count, qr_scan_count, qr_newer_count) = (
          r.get(0),
          r.get(1),
          r.get(2),
          r.get(3),
          r.get(4),
          r.get(5))
        params.+=(Array[Any](app_key, the_day, hour, qr_group_key, qr_visitor_count, qr_scan_count, qr_newer_count, qr_visitor_count, qr_scan_count, qr_newer_count))
      })
      //try {
      JdbcUtil.doBatch(sqlText, params) // 批量入库
      //      }
      //      finally{
      //        statement.close() // 关闭 statement
      //        conn.close()      // 关闭数据库连接
      //      }
    })
  }
}
