package aldwxstat.aldqr

import org.apache.spark.sql.DataFrame

/**
  * 二维码统计分析的接口
  * qr_visitor_count 扫码人数
  * qr_scan_count 扫码次数
  * qr_newer_count 扫码带来新增
  * insert2db 写数据库
  */
trait QrocdeAnalysis {
  def qr_visitor_and_scan_count(df: DataFrame): DataFrame
  def qr_newer_count(df: DataFrame): DataFrame
  def insert2db(df: DataFrame): Unit
}