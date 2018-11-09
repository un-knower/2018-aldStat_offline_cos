package aldwxstat.aldqr

import aldwxutils._
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.mutable.ArrayBuffer


class QrcodeDaily(spark: SparkSession) extends QrocdeAnalysis {

  /**
    * 每天扫码人数,扫码次数
    *
    * @param df read.parquet 数据源
    * @return DataFrame
    */
  def qr_visitor_and_scan_count(df: DataFrame): DataFrame = {
    // 二维码数据源
    val page_df = df.filter("ev='page' and ag_aldsrc !='null' and qr !='null'")
      .select(
        df("ak").alias("app_key"),
        df("qr").alias("qr_key"),
        df("at"),
        df("uu")
      )

    val qr_uv_df = page_df.groupBy("app_key", "qr_key")
      .agg(
        countDistinct("uu") as "total_scan_user_count",
        countDistinct("at") as "total_scan_count"
      )
    qr_uv_df
  }

  /**
    * 每天扫码带来新增
    *
    * @param df read.parquet 数据源
    * @return DataFrame
    */
  def qr_newer_count(df: DataFrame): DataFrame = {
    // 每天访问小程序的新人数
    val ifo_df = df.filter("ev='app' and ifo='true'")
      .select(
        df("ak").alias("app_key"),
        df("uu"),
        df("at")
      )

    // 二维码数据源
    val page_df = df.filter("ev='page' and ag_aldsrc !='null' and qr !='null'")
      .select(
        df("ak").alias("app_key"),
        df("qr").alias("qr_key"),
        df("at"),
        df("uu")
      )

    // 每天扫码带来新增
    val qr_newer_df = page_df.join(
      ifo_df,
      page_df("app_key") === ifo_df("app_key") &&
        page_df("at") === ifo_df("at") &&
        page_df("uu") === ifo_df("uu")
    ).select(
      page_df("app_key"),
      page_df("qr_key"),
      ifo_df("uu")
    ).groupBy("app_key", "qr_key")
      .agg(
        countDistinct("uu").alias("qr_new_comer_for_app")
      )
    qr_newer_df
  }

  /**
    * 每天扫码人数,扫码次数,扫码带来新增入库
    *
    * @param df DataFrame
    */
  def insert2db(df: DataFrame): Unit = {
    val qr_uv_df: DataFrame = qr_visitor_and_scan_count(df)
    val qr_newer_df: DataFrame = qr_newer_count(df)

    // 通过 join 将扫码人数,扫码次数和扫码带来新增形成大的结果 dataframe
    val _result_df = qr_uv_df.join(
      qr_newer_df,
      qr_uv_df("app_key") === qr_newer_df("app_key") &&
        qr_uv_df("qr_key") === qr_newer_df("qr_key"),
      "leftouter"
    ).select(
      qr_uv_df("app_key"),
      qr_uv_df("qr_key"),
      qr_uv_df("total_scan_user_count"),
      qr_uv_df("total_scan_count"),
      qr_newer_df("qr_new_comer_for_app")
    ).na.fill(0)

    qr2mysql(_result_df)
  }

  private def qr2mysql(qrDf: DataFrame) = {
    //qrDf.show()
    // 逐行入库
    val the_day = ArgsTool.day // 日期
    val update_at = TimeUtil.nowInt() // 数据更新时间
    qrDf.foreachPartition((rows: Iterator[Row]) => {
      //      val conn = JdbcUtil.getConn()
      //      val statement = conn.createStatement
      val sqlText = "insert into aldstat_qr_code_statistics (app_key,day,qr_key,total_scan_user_count,total_scan_count,qr_new_comer_for_app, update_at) values (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE total_scan_user_count=?, total_scan_count=?, qr_new_comer_for_app=?, update_at=?"
      val params = new ArrayBuffer[Array[Any]]()

      for (r <- rows) {
        val (app_key, qr_key, total_scan_user_count, total_scan_count, qr_new_comer_for_app) = (
          r.get(0),
          r.get(1),
          r.get(2),
          r.get(3),
          r.get(4)
        )
        params.+=(Array[Any](
          app_key,
          the_day,
          qr_key,
          total_scan_user_count,
          total_scan_count,
          qr_new_comer_for_app,
          update_at,
          total_scan_user_count,
          total_scan_count,
          qr_new_comer_for_app,
          update_at))
      }
      JdbcUtil.doBatch(sqlText, params)
      //      statement.close()
      //      conn.close()
    })
  }
}

