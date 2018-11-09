package aldwxstat



import aldwxconfig.ConfigurationUtil
import aldwxutils.{ArgsTool, EmojiFilter, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{count, max}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaoxiang on 2018/1/11.
  */
object Error {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val ss = SparkSession.builder()
      .appName(this.getClass.getName)
      .getOrCreate()
    //获取昨天时间
    val yesterday = aldwxutils.TimeUtil.processArgs(args)
    allErrorStatistics(ss, yesterday, args)
    ss.close()
  }

  def allErrorStatistics(spark: SparkSession, yesterday: String, args: Array[String]): Unit = {
    //==========================================================1
    /*
    *gcs:
    *从我们的log日志中读取7天的数据
    */
    //    val errorDF = Aldstat_args_tool.analyze_args(args, spark, DBConf.errorPath).filter("ak != 'null' and ak != ''")
    val errorDF = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("error.parquet")).filter("ak != 'null' and ak != ''")
    println("打印今天日志")
    errorDF.show()

    //==========================================================2
    /*
    *gcs:
    *
    */
    if (0 == args.length) {
      val openDF = JdbcUtil.readFromMysql(spark, s"(select app_key,open_count from aldstat_trend_analysis where day='$yesterday') as trend_df")
      new allErrorCount(spark, yesterday, errorDF, args, openDF).errorCount()
    }
    for (i <- 0 until args.length) {
      //==========================================================3
      /*
      *gcs:
      *
      */
      if ("-d".equals(args(i))) {
        val openDF = JdbcUtil.readFromMysql(spark, s"(select app_key,open_count from aldstat_trend_analysis where day='$yesterday') as trend_df")
        new allErrorCount(spark, yesterday, errorDF, args, openDF).errorCount()

      } else if ("-du".equals(args(i))) {
        //==========================================================4
        /*
        *gcs:
        *如果-du 是7天的话
        */
        if (i + 1 < args.length && args(i + 1).equals("7")) {
          //==========================================================5
          /*
          *gcs:
          *如果是7天的数据，就会执行7天的数据
          */
          val openDF = JdbcUtil.readFromMysql(spark, s"(select app_key,open_count from aldstat_7days_trend_analysis where day='$yesterday') as trend_df")
          //==========================================================6
          /*
          *gcs:
          *之后就执行 allErrorCount 这个函数
          */
          new allErrorCount(spark, yesterday, errorDF, args, openDF).errorCount()
        } else if (i + 1 < args.length && args(i + 1).equals("30")) {
          val openDF = JdbcUtil.readFromMysql(spark, s"(select app_key,open_count from aldstat_30days_trend_analysis where day='$yesterday') as trend_df")
          new allErrorCount(spark, yesterday, errorDF, args, openDF).errorCount()
        }
      }
    }
  }
}

class allErrorCount(spark: SparkSession, yesterday: String, errorDF: Dataset[Row], args: Array[String], openDF: DataFrame) extends Serializable {
  def errorCount(): Unit = {
    //==========================================================7
    /*
    *gcs:
    *计算错误的次数
    */
    val errorForWV = errorDF.select(
      errorDF("ak"),
      errorDF("st"),
      errorDF("wv"), //微信版本
      errorDF("v"), //v 小程序
      errorDF("ct")
    ).groupBy("ak", "wv", "v", "ct")
      .agg(
        max("st").alias("max_st"),
        count("ct").alias("err_count")
      )

    //总的错误数
    val errorAll = errorDF.select(
      errorDF("ak"),
      errorDF("ct")
    ).groupBy("ak", "ct")
      .agg(
        count("ct").alias("err_sum_count")
      )


    //读取179 上面的打开次数  趋势分析表中 aldstat_trend_analysis
    //val df_3 = DBConf.read_from_mysql(spark, s"(select app_key,open_count from aldstat_trend_analysis where day='$yesterday') as trend_df")

    //==========================================================8
    /*
    *gcs:
    *将DF中需要提取出来的字段提取出来的ak,wv,v,ct等字段提取出来
    */
    val df_4 = errorForWV.join(errorAll, errorForWV("ak") === errorAll("ak") &&
      errorForWV("ct") === errorAll("ct"))
      .select(
        errorForWV("ak"),
        errorForWV("wv"),
        errorForWV("v"),
        errorForWV("ct"),
        errorForWV("max_st"),
        errorForWV("err_count"),
        errorAll("err_sum_count")
      )

    //==========================================================9
    /*
    *gcs:
    *将ak和app_key都提取出来
    */
    val rs_df = df_4.join(openDF, df_4("ak") === openDF("app_key"))
      .select(
        df_4("ak"),
        df_4("wv"), //微信版本
        df_4("v"), //sdk 版本
        df_4("ct"),
        df_4("max_st"), //最近出现的时间
        df_4("err_count"), //版本错误次数
        df_4("err_sum_count"), //总错误次数
        openDF("open_count") //打开次数
      ).na.fill(0).createTempView("table")

    //次数占比   版本错误数/总错误数
    val result = spark.sql("select ak,wv,v,max_st,err_count,err_sum_count,cast(err_count/err_sum_count as float),open_count,now(),ct from table")
    new errorInsert2Mysql(result, yesterday, args).insertmysql()
    result.show()
  }

}

class errorInsert2Mysql(result: DataFrame, yesterday: String, args: Array[String]) extends Serializable {
  def insertmysql(): Unit = {
    //    val conn = MySqlPool.getJdbcConn()
    //      statement = conn.createStatement()
    //      conn.setAutoCommit(false)
    //      row.foreach(r => {
    //        try {
    //          val app_key = r(0)
    //          val day = yesterday
    //          val wv = r(1)
    //          val v = r(2)
    //          val max_st = r(3).toString
    //          val err_count = r(4)
    //          val err_sum_count = r(5)
    //          val error_ratio = r(6)
    //          val open_count = r(7)
    //          val update_at = r(8)
    //          var ct = r(9)
    //          ct = ct.toString.replaceAll("'", "''") //将 单引号 转义
    //          if (ct.toString.length > 255) {
    //            ct = ct.toString.substring(0, 254)
    //          } else {
    //            ct = ct.toString.substring(0, ct.toString.length - 1)
    //          }
    //          if (0 == args.length) {
    //            val sql = "insert into aldstat_daily_error (app_key,day,error_time,wechart_version,sdk_version,error_count,err_sum_count,open_count,error_ratio,error_message,update_at)" + s"values ('${app_key}','${day}','${max_st}','${wv}','${v}','${err_count}','${err_sum_count}','${open_count}','${error_ratio}','${ct}','${update_at}') ON DUPLICATE KEY UPDATE error_count='${err_count}',open_count='${open_count}', err_sum_count='${err_sum_count}',error_ratio='${error_ratio}', update_at='${update_at}'"
    //            statement.addBatch(sql)
    //            statement.executeBatch
    //            conn.commit()
    //          }
    //          for (i <- 0 until args.length) {
    //            if ("-d".equals(args(i))) {
    //              val sql = "insert into aldstat_daily_error (app_key,day,error_time,wechart_version,sdk_version,error_count,err_sum_count,open_count,error_ratio,error_message,update_at)" + s"values ('${app_key}','${day}','${max_st}','${wv}','${v}','${err_count}','${err_sum_count}','${open_count}','${error_ratio}','${ct}','${update_at}') ON DUPLICATE KEY UPDATE error_count='${err_count}',open_count='${open_count}', err_sum_count='${err_sum_count}',error_ratio='${error_ratio}', update_at='${update_at}'"
    //              statement.addBatch(sql)
    //              statement.executeBatch
    //              conn.commit()
    //
    //            } else if ("-du".equals(args(i))) {
    //              if (i + 1 < args.length && args(i + 1).equals("7")) {
    //                val sql = "insert into aldstat_7days_error (app_key,day,error_time,wechart_version,sdk_version,error_count,err_sum_count,open_count,error_ratio,error_message,update_at)" + s"values ('${app_key}','${day}','${max_st}','${wv}','${v}','${err_count}','${err_sum_count}','${open_count}','${error_ratio}','${ct}','${update_at}') ON DUPLICATE KEY UPDATE error_count='${err_count}',open_count='${open_count}', err_sum_count='${err_sum_count}',error_ratio='${error_ratio}', update_at='${update_at}'"
    //                statement.addBatch(sql)
    //                statement.executeBatch
    //                conn.commit()
    //              } else if (i + 1 < args.length && args(i + 1).equals("30")) {
    //                val sql = "insert into aldstat_30days_error (app_key,day,error_time,wechart_version,sdk_version,error_count,err_sum_count,open_count,error_ratio,error_message,update_at)" + s"values ('${app_key}','${day}','${max_st}','${wv}','${v}','${err_count}','${err_sum_count}','${open_count}','${error_ratio}','${ct}','${update_at}') ON DUPLICATE KEY UPDATE error_count='${err_count}',open_count='${open_count}', err_sum_count='${err_sum_count}',error_ratio='${error_ratio}', update_at='${update_at}'"
    //                statement.addBatch(sql)
    //                statement.executeBatch
    //                conn.commit()
    //              }
    //            }
    //          }
    //        } catch {
    //          case e: Exception => e.printStackTrace()
    //          case _: Exception => println("1入库出现异常了---", println(r.toString()))
    //
    //        }
    //
    //      })
    result.foreachPartition((row: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      var sqlText = ""
      row.foreach(r => {
        val app_key = r.get(0)
        val day = yesterday
        val wv = r.get(1)
        val v = r.get(2)
        val max_st = r.get(3).toString
        val err_count = r.get(4)
        val err_sum_count = r.get(5)
        val error_ratio = r.get(6)
        val open_count = r.get(7)
        val update_at = r.get(8)
        var ct = r.get(9).toString
        ct = ct.replaceAll("'", "''") //将 单引号 转义
        if (ct.length > 255) {
          ct = ct.substring(0, 254)
        } else {
          ct = ct.substring(0, ct.length - 1)
        }
        val ef=new EmojiFilter()
        if(ef.isSpecial(ct)){
          ct=ef.filterSpecial(ct)
        }

        if (0 == args.length) {
          sqlText = "insert into aldstat_daily_error (app_key,day,error_time,wechart_version,sdk_version,error_count,err_sum_count,open_count,error_ratio,error_message,update_at)" +
            s"values (?,?,?,?,?,?,?,?,?,?,?) " +
            s"ON DUPLICATE KEY UPDATE error_count=?,open_count=?, err_sum_count=?,error_ratio=?, update_at=?"
          params.+=(Array[Any](app_key, day, max_st, wv, v, err_count, err_sum_count, open_count, error_ratio, ct, update_at, err_count, open_count, err_sum_count, error_ratio, update_at))
        }
        for (i <- 0 until args.length) {
          if ("-d".equals(args(i))) {
            sqlText = "insert into aldstat_daily_error (app_key,day,error_time,wechart_version,sdk_version,error_count,err_sum_count,open_count,error_ratio,error_message,update_at)" +
              s"values (?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE error_count=?,open_count=?, err_sum_count=?,error_ratio=?, update_at=?"
            params.+=(Array[Any](app_key, day, max_st, wv, v, err_count, err_sum_count, open_count, error_ratio, ct, update_at, err_count, open_count, err_sum_count, error_ratio, update_at))

          } else if ("-du".equals(args(i))) {
            if (i + 1 < args.length && args(i + 1).equals("7")) {
              sqlText = "insert into aldstat_7days_error (app_key,day,error_time,wechart_version,sdk_version,error_count,err_sum_count,open_count,error_ratio,error_message,update_at)" +
                s"values (?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE error_count=?,open_count=?, err_sum_count=?,error_ratio=?, update_at=?"
              params.+=(Array[Any](app_key, day, max_st, wv, v, err_count, err_sum_count, open_count, error_ratio, ct, update_at, err_count, open_count, err_sum_count, error_ratio, update_at))
            } else if (i + 1 < args.length && args(i + 1).equals("30")) {
              sqlText = "insert into aldstat_30days_error (app_key,day,error_time,wechart_version,sdk_version,error_count,err_sum_count,open_count,error_ratio,error_message,update_at)" +
                s"values (?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE error_count=?,open_count=?, err_sum_count=?,error_ratio=?, update_at=?"
              params.+=(Array[Any](app_key, day, max_st, wv, v, err_count, err_sum_count, open_count, error_ratio, ct, update_at, err_count, open_count, err_sum_count, error_ratio, update_at))

            }
          }
        }
      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })
  }
}



