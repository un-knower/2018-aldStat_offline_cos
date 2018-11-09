package aldwxstat

import aldwxconfig.ConfigurationUtil
import aldwxutils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangyanpeng on 2017/8/22.
  * 受访页的页面统计
  *
  * 详情表
  *
  * 退出率：退出页次数/访问次数
  * 平均访问时长：总停留时长/打开次数
  */
object PageViewDaily {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "zhangyanpeng")
    //获取昨天的日期
    val yesterday = TimeUtil.processArgs(args)
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      //.config("spark.sql.shuffle.partitions", 12)
      //.master("local")
      .getOrCreate()

    //    val df = Aldstat_args_tool.analyze_args(args,sparkSession,DBConf.hdfsUrl).filter("ak!='null' and ev='page'").cache()
    val df = ArgsTool.getLogs(args, sparkSession, ConfigurationUtil.getProperty("tongji.parquet")).filter("ak!='null' and ev='page'").cache()

    //  val df = ArgsTool.getLogs(args, sparkseesion,ConfigurationUtil.getProperty("tongji.parquet"), "7")
    df.createTempView("page")
    //获得 每个at 最大st
    val df_1 = df.select(
      df("ak"),
      df("pp"),
      df("at"),
      df("st")
    ).groupBy("ak", "at")
      .agg(
        max("st") as "mst"
      )

    //受访页 的退出数
    val df_2 = df.join(df_1, df("ak") === df_1("ak") &&
      //df("pp") === df_1("pp") &&
      df("at") === df_1("at") &&
      df("st") === df_1("mst")
    ).select(
      df("ak"),
      df("pp"),
      df("lp"),
      df("at")
    ).groupBy("ak", "pp")
      .agg(
        count("lp") as "exit_count"
      ).distinct()

    //获得 访问人数  访问次数   打开次数
    val df_3 = df.filter(df("ev") === "page").select(
      df("ak"),
      df("pp"),
      df("uu"),
      df("at")
    ).groupBy("ak", "pp")
      .agg(
        countDistinct("uu") as "count_uu",
        count("pp") as "count_pp",
        countDistinct("at") as "count_at"
      ).distinct()


    val df_4 = df.select(
      df("ak"),
      df("pp"),
      df("at"),
      df("st")
    ).groupBy("ak", "pp", "at")
      .agg(
        max("st") as "mst",
        min("st") as "ist"
      )
      .select("ak", "pp", "mst", "ist")
      .distinct()

    val df_5 = df_4.select(
      df_4("ak"),
      df_4("pp"),
      ((df_4("mst") - df_4("ist")) / 1000) as "time"
    ).groupBy("ak", "pp")
      .agg(
        sum("time") as "total"
      )

    val df_6 = df_5.join(df_3, df_3("ak") === df_5("ak") &&
      df_3("pp") === df_5("pp")
    ).select(
      df_3("ak") as "ak",
      df_3("pp") as "pp",
      df_3("count_uu") as "count_uu",
      df_3("count_pp") as "count_pp",
      df_3("count_at") as "count_at",
      df_5("total") // 总时长
    )

    val rs = df_6.join(df_2, df_6("ak") === df_2("ak") &&
      df_6("pp") === df_2("pp"), "left_outer")
      .select(
        df_6("ak") as ("ak"),
        df_6("pp") as ("pp"),
        df_6("count_uu") as ("count_uu"),
        df_6("count_pp") as ("count_pp"),
        df_6("count_at") as ("count_at"),
        df_6("total") as ("total_time"),
        df_6("total") / df_6("count_at") as ("avg_time"),
        df_2("exit_count") / df_6("count_pp") as ("bounce_rate"),
        df_2("exit_count") as ("abort_page_count")
      ).na.fill(0).distinct()

    rs.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_page_view (app_key,day,page_path,page_count,abort_page_count,visitor_count,open_count,total_stay_time,avg_stay_time,abort_ratio,update_at) values (?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE page_count=?,visitor_count=?,open_count=?,total_stay_time=?,avg_stay_time=?,abort_ratio=?,update_at=?,abort_page_count=?"

      rows.foreach(r => {
        val app_key = r.get(0)
        val day = yesterday
        val page_path = r.get(1)
        val visitor_count = r.get(2) //访问人数
        val page_count = r.get(3) //访问次数
        val open_count = r.get(4) //打开次数
        val total_time = r.get(5) //总停留时长
        val avg_stay_time = r.get(6) //次均停留时长
        val bounce_rate = r.get(7) //跳出率
        val abort_page_count = r.get(8) //跳出页个数
        //          val share_count=0                 //分享次数
        val update_at = TimeUtil.nowInt()
        // ON DUPLICATE KEY UPDATE page_count='${page_count}',visitor_count='${visitor_count}',open_count='${open_count}',total_stay_time='${total_time}',
        // avg_stay_time='${avg_stay_time}',abort_ratio='${bounce_rate}',update_at='${update_at}',abort_page_count='${abort_page_count}'"
        params.+=(Array[Any](app_key, day, page_path, page_count, abort_page_count, visitor_count, open_count, total_time, avg_stay_time, bounce_rate, update_at,
          page_count, visitor_count, open_count, total_time, avg_stay_time, bounce_rate, update_at, abort_page_count))
      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })

    sparkSession.stop()
  }


}
