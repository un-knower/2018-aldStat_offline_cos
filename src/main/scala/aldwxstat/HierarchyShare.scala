package aldwxstat

import java.sql.Connection
import java.util.Properties

import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaoxiang on 2018/1/10.
  */
object HierarchyShare {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val ss = SparkSession.builder()
      .appName(this.getClass.getName)
      .getOrCreate()
    //获取昨天时间
    val yesterday = aldwxutils.TimeUtil.processArgs(args)
    allHierarchyShare(ss, yesterday, args)
    ss.close()
  }


  def allHierarchyShare(spark: SparkSession, yesterday: String, args: Array[String]): logicHierarchyShare = {
    ArgsTool.analysisArgs(args)
    val du = ArgsTool.du
    //    val shareDF = Aldstat_args_tool.analyze_args(args, spark, DBConf.hdfsUrl, "etl-ald-log*")
//    val shareDF = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet"))

//     val shareDF=ArgsTool.getTencentDailyDataFrame(spark, ConfigurationUtil.getProperty("tencent.parquet"))
    val shareDF= ArgsTool.getTencentSevenOrThirty(spark,du)
    shareDF.createTempView("t_share")
    new logicHierarchyShare(spark, yesterday, shareDF, args)

  }
}

class logicHierarchyShare(ss: SparkSession, yesterday: String, shareDF: DataFrame, args: Array[String]) extends Serializable {

  def mysplituuid(s: String): String = {
    val ss = s.split(",")
    val uuid = ss(1)
    uuid
  }

  if (shareDF.columns.contains("ct_chain")) {
    //一度分享人数和次数
    val fc = ss.sql("SELECT ak app_key,COUNT(DISTINCT uu) share_user,COUNT(ct_chain) share_count FROM" +
      " t_share WHERE ev='event' and  ct!= 'fail' and tp='ald_share_chain' AND ct_chain NOT LIKE CONCAT('%,%') AND ct_chain !='null' GROUP BY ak")
    fc.createTempView("o_a")
    //一度分享带来的新增
    ss.sql("select ak app_key , count(DISTINCT uu) as snuc from" +
      " t_share WHERE tp='ald_share_chain' AND ev ='event'AND ct_chain NOT  LIKE CONCAT('%,%')   " +
      "AND uu in (SELECT uu FROM t_share where ev='app' and ifo='true') " +
      " GROUP BY ak").createTempView("o_b")
    ss.udf.register("split", mysplituuid _)
    //二度分享人数和次数
    ss.sql("SELECT ak app_key,COUNT(ct_chain) share_count2,COUNT(DISTINCT share_uuid) share_user2 FROM " +
      " (SELECT ak ,ct_chain,split(ct_chain) AS share_uuid FROM t_share WHERE ct_chain  LIKE CONCAT('%,%') AND length(ct_chain) >25 AND ev ='event' GROUP BY ak,ct_chain)" +
      " GROUP BY ak").createTempView("t_a")
    //二度分享带来的新增
    val tuc = ss.sql("select ak app_key , count(DISTINCT uu) as snuc2 from" +
      " t_share WHERE tp='ald_share_chain' AND ev ='event' AND ct_chain  LIKE CONCAT('%,%') AND length(ct_chain) >25  " +
      "AND uu in (SELECT uu FROM t_share where ev='app' and ifo='true') " +
      " GROUP BY ak").createTempView("t_b")
    //一度分享的回流量
    val fbf = ss.sql("select ak app_key ,count(uu) backflow from t_share where  tp='ald_share_click'" +
      " and ev ='event' and wsr_query_ald_share_src NOT LIKE CONCAT('%,%')  group by ak").createTempView("o_c")
    ss.udf.register("split", mysplituuid _)
    //二度分享回流量
    val aa = ss.sql("select ak app_key ,count(uu) 2backflow from t_share where  tp='ald_share_click'" +
      " and ev ='event' and wsr_query_ald_share_src  LIKE CONCAT('%,%')  group by ak").createTempView("t_c")
    //一度分享临时表
    ss.sql("SELECT o_a.app_key ,o_a.share_user ,o_a.share_count ,o_b.snuc,o_c.backflow FROM o_a LEFT JOIN " +
      "o_b ON o_a.app_key =o_b.app_key LEFT JOIN o_c ON o_a.app_key =o_c.app_key").na.fill(0).createTempView("one_share")
    //二度分享临时表
    ss.sql("SELECT t_a.app_key ,t_a.share_user2 ,t_a.share_count2 ,t_b.snuc2,t_c.2backflow FROM t_a LEFT JOIN " +
      "t_b ON t_a.app_key =t_b.app_key LEFT JOIN t_c ON t_a.app_key =t_c.app_key").na.fill(0).createTempView("two_share")
    val result = ss.sql("SELECT one_share.app_key," + yesterday + " day ,one_share.share_user,one_share.share_count,one_share.snuc,one_share.backflow, " +
      " one_share.backflow / one_share.share_count AS frist_backflow_ratio," +
      "two_share.share_user2,two_share.share_count2,two_share.snuc2,two_share.2backflow ," +
      " two_share.2backflow / two_share.share_count2 AS two_backflow_ratio, " +
      "unix_timestamp() as update_at  " +
      "FROM one_share LEFT JOIN two_share ON one_share.app_key=two_share.app_key").na.fill(0)
    new insert2Mysql(result, args)
  }
}

class insert2Mysql(result: DataFrame, args: Array[String]) extends Serializable {
  //  val url = DBConf.url
  //  val prop = new Properties()
  //  prop.put("driver", DBConf.driver)
  //  prop.setProperty("user", DBConf.user)
  //  prop.setProperty("password", DBConf.password)
  val url = ConfigurationUtil.getProperty("jdbc.url")
  val prop = new Properties()
  prop.put("driver", ConfigurationUtil.getProperty("jdbc.driver"))
  prop.setProperty("user", ConfigurationUtil.getProperty("jdbc.user"))
  prop.setProperty("password", ConfigurationUtil.getProperty("jdbc.pwd"))
  var connection: Connection = null
  result.foreachPartition((rows: Iterator[Row]) => {

//    val conn = JdbcUtil.getConn()
//    val statement = conn.createStatement
    val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
    var sqlText = ""

    rows.foreach(r => {
      val app_key = r(0)
      val day = r(1)
      val first_share_count = r(3)
      val first_share_user_count = r(2)
      val first_share_new_user_count = r(4)
      val frist_backflow = r(5)
      val frist_backflow_ratio = r(6)
      val secondary_share_count = r(8)
      val secondary_share_user_count = r(7)
      val secondary_share_new_user_count = r(9)
      val secondary_backflow = r(10)
      val secondary_backflow_ratio = r(11)
      val update_at = r(12)
      //             println("进入输出")

      //      try {
      //        Class.forName(DBConf.driver)
      //        connection = DriverManager.getConnection(url, DBConf.user, DBConf.password)
      //        val statement = connection.createStatement
      //        if(0==args.length){
      //          val sql = s"insert into ald_hierarchy_share (app_key,day,first_share_count,first_share_user_count," +
      //            s" first_share_new_user_count,secondary_share_count,secondary_share_user_count,update_at," +
      //            s"frist_backflow, frist_backflow_ratio, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio )" +
      //            s"values ('${app_key}','${day}','${first_share_count}','${first_share_user_count}','${first_share_new_user_count}','${secondary_share_count}','${secondary_share_user_count}','${update_at}','${frist_backflow}','${frist_backflow_ratio}','${secondary_share_new_user_count}','${secondary_backflow}','${secondary_backflow_ratio}') ON " +
      //            s"DUPLICATE KEY UPDATE secondary_share_count='${secondary_share_count}', update_at='${update_at}', secondary_share_user_count='${secondary_share_user_count}',first_share_count='${first_share_count}',first_share_user_count='${first_share_user_count}',first_share_new_user_count='${first_share_new_user_count}'" +
      //            s",frist_backflow='${frist_backflow}',frist_backflow_ratio='${frist_backflow_ratio}',secondary_backflow='${secondary_backflow}',secondary_backflow_ratio='${secondary_backflow_ratio}',secondary_share_new_user_count='${secondary_share_new_user_count}'"
      //          val rs = statement.executeUpdate(sql)
      //        }
      //        for (i <- 0 until args.length) {
      //          if ("-d".equals(args(i))) {
      //            val sql = s"insert into ald_hierarchy_share (app_key,day,first_share_count,first_share_user_count," +
      //              s" first_share_new_user_count,secondary_share_count,secondary_share_user_count,update_at," +
      //              s"frist_backflow, frist_backflow_ratio, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio )" +
      //              s"values ('${app_key}','${day}','${first_share_count}','${first_share_user_count}','${first_share_new_user_count}','${secondary_share_count}','${secondary_share_user_count}','${update_at}','${frist_backflow}','${frist_backflow_ratio}','${secondary_share_new_user_count}','${secondary_backflow}','${secondary_backflow_ratio}') ON " +
      //              s"DUPLICATE KEY UPDATE secondary_share_count='${secondary_share_count}', update_at='${update_at}', secondary_share_user_count='${secondary_share_user_count}',first_share_count='${first_share_count}',first_share_user_count='${first_share_user_count}',first_share_new_user_count='${first_share_new_user_count}'" +
      //              s",frist_backflow='${frist_backflow}',frist_backflow_ratio='${frist_backflow_ratio}',secondary_backflow='${secondary_backflow}',secondary_backflow_ratio='${secondary_backflow_ratio}',secondary_share_new_user_count='${secondary_share_new_user_count}'"
      //            val rs = statement.executeUpdate(sql)
      //          }
      //          if ("-du".equals(args(i))) {
      //            if (i + 1 < args.length && args(i + 1).equals("7")) {
      //              val sql = s"insert into ald_7days_hierarchy_share (app_key,day,first_share_count,first_share_user_count," +
      //                s" first_share_new_user_count,secondary_share_count,secondary_share_user_count,update_at," +
      //                s"frist_backflow, frist_backflow_ratio, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio )" +
      //                s"values ('${app_key}','${day}','${first_share_count}','${first_share_user_count}','${first_share_new_user_count}','${secondary_share_count}','${secondary_share_user_count}','${update_at}','${frist_backflow}','${frist_backflow_ratio}','${secondary_share_new_user_count}','${secondary_backflow}','${secondary_backflow_ratio}') ON " +
      //                s"DUPLICATE KEY UPDATE secondary_share_count='${secondary_share_count}', update_at='${update_at}', secondary_share_user_count='${secondary_share_user_count}',first_share_count='${first_share_count}',first_share_user_count='${first_share_user_count}',first_share_new_user_count='${first_share_new_user_count}'" +
      //                s",frist_backflow='${frist_backflow}',frist_backflow_ratio='${frist_backflow_ratio}',secondary_backflow='${secondary_backflow}',secondary_backflow_ratio='${secondary_backflow_ratio}',secondary_share_new_user_count='${secondary_share_new_user_count}'"
      //              val rs = statement.executeUpdate(sql)
      //            } else if (i + 1 < args.length && args(i + 1).equals("30")) {
      //              val sql = s"insert into ald_30days_hierarchy_share (app_key,day,first_share_count,first_share_user_count," +
      //                s" first_share_new_user_count,secondary_share_count,secondary_share_user_count,update_at," +
      //                s"frist_backflow, frist_backflow_ratio, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio )" +
      //                s"values ('${app_key}','${day}','${first_share_count}','${first_share_user_count}','${first_share_new_user_count}','${secondary_share_count}','${secondary_share_user_count}','${update_at}','${frist_backflow}','${frist_backflow_ratio}','${secondary_share_new_user_count}','${secondary_backflow}','${secondary_backflow_ratio}') ON " +
      //                s"DUPLICATE KEY UPDATE secondary_share_count='${secondary_share_count}', update_at='${update_at}', secondary_share_user_count='${secondary_share_user_count}',first_share_count='${first_share_count}',first_share_user_count='${first_share_user_count}',first_share_new_user_count='${first_share_new_user_count}'" +
      //                s",frist_backflow='${frist_backflow}',frist_backflow_ratio='${frist_backflow_ratio}',secondary_backflow='${secondary_backflow}',secondary_backflow_ratio='${secondary_backflow_ratio}',secondary_share_new_user_count='${secondary_share_new_user_count}'"
      //              val rs = statement.executeUpdate(sql)
      //            }

      //          }
      //        }

      //      } catch {
      //        case e: Exception => e.printStackTrace()
      //          connection.close()
      //      }
      if (0 == args.length) {

        sqlText = s"insert into ald_hierarchy_share (app_key,day,first_share_count,first_share_user_count," +
          s" first_share_new_user_count,secondary_share_count,secondary_share_user_count,update_at," +
          s"frist_backflow, frist_backflow_ratio, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio )" +
          s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
          s"DUPLICATE KEY UPDATE secondary_share_count=?, update_at=?, secondary_share_user_count=?,first_share_count=?,first_share_user_count=?,first_share_new_user_count=?" +
          s",frist_backflow=?,frist_backflow_ratio=?,secondary_backflow=?,secondary_backflow_ratio=?,secondary_share_new_user_count=?"

        params.+=(Array[Any](app_key, day, first_share_count, first_share_user_count, first_share_new_user_count, secondary_share_count, secondary_share_user_count, update_at, frist_backflow, frist_backflow_ratio, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio, secondary_share_count, update_at, secondary_share_user_count, first_share_count, first_share_user_count, first_share_new_user_count, frist_backflow, frist_backflow_ratio, secondary_backflow, secondary_backflow_ratio, secondary_share_new_user_count))
      }
      for (i <- 0 until args.length) {
        if ("-d".equals(args(i))) {

          sqlText = s"insert into ald_hierarchy_share (app_key,day,first_share_count,first_share_user_count," +
            s" first_share_new_user_count,secondary_share_count,secondary_share_user_count,update_at," +
            s"frist_backflow, frist_backflow_ratio, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio )" +
            s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
            s"DUPLICATE KEY UPDATE secondary_share_count=?, update_at=?, secondary_share_user_count=?,first_share_count=?,first_share_user_count=?,first_share_new_user_count=?" +
            s",frist_backflow=?,frist_backflow_ratio=?,secondary_backflow=?,secondary_backflow_ratio=?,secondary_share_new_user_count=?"
          params.+=(Array[Any](app_key, day, first_share_count, first_share_user_count, first_share_new_user_count, secondary_share_count, secondary_share_user_count, update_at, frist_backflow, frist_backflow_ratio, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio, secondary_share_count, update_at, secondary_share_user_count, first_share_count, first_share_user_count, first_share_new_user_count, frist_backflow, frist_backflow_ratio, secondary_backflow, secondary_backflow_ratio, secondary_share_new_user_count))
        }
        if ("-du".equals(args(i))) {
          if (i + 1 < args.length && args(i + 1).equals("7")) {

            sqlText = s"insert into ald_7days_hierarchy_share (app_key,day,first_share_count,first_share_user_count," +
              s" first_share_new_user_count,secondary_share_count,secondary_share_user_count,update_at," +
              s"frist_backflow, frist_backflow_ratio, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio )" +
              s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
              s"DUPLICATE KEY UPDATE secondary_share_count=?, update_at=?, secondary_share_user_count=?,first_share_count=?,first_share_user_count=?,first_share_new_user_count=?" +
              s",frist_backflow=?,frist_backflow_ratio=?,secondary_backflow=?,secondary_backflow_ratio=?,secondary_share_new_user_count=?"
            params.+=(Array[Any](app_key, day, first_share_count, first_share_user_count, first_share_new_user_count, secondary_share_count, secondary_share_user_count, update_at, frist_backflow, frist_backflow_ratio, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio, secondary_share_count, update_at, secondary_share_user_count, first_share_count, first_share_user_count, first_share_new_user_count, frist_backflow, frist_backflow_ratio, secondary_backflow, secondary_backflow_ratio, secondary_share_new_user_count))
          } else if (i + 1 < args.length && args(i + 1).equals("30")) {
            sqlText = s"insert into ald_30days_hierarchy_share (app_key,day,first_share_count,first_share_user_count," +
              s" first_share_new_user_count,secondary_share_count,secondary_share_user_count,update_at," +
              s"frist_backflow, frist_backflow_ratio, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio )" +
              s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
              s"DUPLICATE KEY UPDATE secondary_share_count=?, update_at=?, secondary_share_user_count=?,first_share_count=?,first_share_user_count=?,first_share_new_user_count=?" +
              s",frist_backflow=?,frist_backflow_ratio=?,secondary_backflow=?,secondary_backflow_ratio=?,secondary_share_new_user_count=?"
            params.+=(Array[Any](app_key, day, first_share_count, first_share_user_count, first_share_new_user_count, secondary_share_count, secondary_share_user_count, update_at, frist_backflow, frist_backflow_ratio, secondary_share_new_user_count, secondary_backflow, secondary_backflow_ratio, secondary_share_count, update_at, secondary_share_user_count, first_share_count, first_share_user_count, first_share_new_user_count, frist_backflow, frist_backflow_ratio, secondary_backflow, secondary_backflow_ratio, secondary_share_new_user_count))
          }
        }
      }
    })
//    try {
      JdbcUtil.doBatch(sqlText, params) //批量入库
//    }
//    finally {
//      statement.close() //关闭statement
//      conn.close() //关闭数据库连接
//    }
  })
}



