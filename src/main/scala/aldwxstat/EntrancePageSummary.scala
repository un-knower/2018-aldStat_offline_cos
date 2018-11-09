package aldwxstat

import java.sql.Statement

import aldwxconfig.ConfigurationUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import aldwxutils._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangyanpeng on 2017/8/22.
  *
  * 入口指标分析   ev=page and ifp=true => 表示有入口页面
  *
  * 汇总表
  */

object EntrancePageSummary {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "zhangyanpeng")
    //获取昨天的日期
    val yesterday = TimeUtil.processArgs(args)
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .config("spark.sql.shuffle.partitions", 12)
      .getOrCreate()
    val df = ArgsTool.getLogs(args, sparkSession, ConfigurationUtil.getProperty("tongji.parquet")).filter("ev='page' and ak!=''").cache()
    df.createTempView("jumpDemo")

    //获得入口页的访问人数  入口页次数
    val df_1 = df.filter(df("ifp") === "true")
      .select(
        df("ak"),
        df("uu"),
        df("at"),
        df("pp")
      ).groupBy("ak")
      .agg(
        countDistinct("uu") as "count_uu", //访问人数
        count("pp") as "count_pp" //入口页次数
      ).distinct()

    //总的打开次数
    sparkSession.sql("SELECT ak,COUNT(DISTINCT at) open FROM jumpDemo where ifp='true' GROUP BY ak").createTempView("open")
    //总的访问次数
    sparkSession.sql("SELECT ak,COUNT(pp) open FROM jumpDemo GROUP BY ak").createTempView("view")
    //总的停留时长
    sparkSession.sql("SELECT ak,sum(time) total_time FROM (SELECT ak,pp,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) time FROM jumpDemo GROUP BY ak,pp,at) group by ak").createTempView("time")

    //统计只有一个访问页（跳失的入口页）
    sparkSession.sql("select t.ak ak, sum(t.aat) exit from (SELECT ak, at ,COUNT(at) aat,pp FROM jumpDemo where ifp='true' GROUP BY ak,at,pp having count(pp)=1) t group by t.ak").createTempView("one_page")

    //次均停留时长   受访页的访问次数
    val staytime = sparkSession.sql("select o.ak,t.total_time,cast(t.total_time/o.open as float)as avg_stay_time,o.open count_at from open o left join time t on o.ak=t.ak")
    //跳失率
    val jump = sparkSession.sql("select v.ak,o.exit,cast(o.exit/v.open as float) as bounce_rate,v.open view_open from view v left join one_page o on v.ak=o.ak")


    val df_2 = staytime.join(jump, staytime("ak") === jump("ak")).select(
      staytime("ak"),
      staytime("total_time"), //总停留时长
      staytime("avg_stay_time"), //次均停留时长
      jump("exit"), //跳出页个数
      jump("bounce_rate"), //跳失率
      jump("view_open"), //访问次数
      staytime("count_at") //打开次数
    ).distinct()

    val result = df_1.join(df_2, df_1("ak") === df_2("ak")).select(
      df_1("ak"),
      df_1("count_pp"), //入口页次数
      df_1("count_uu"), //访问人数
      df_2("view_open"), //访问次数
      df_2("total_time"), //总停留时长
      df_2("avg_stay_time"), //次均停留时长
      df_2("exit"), //跳出页个数
      df_2("bounce_rate"), //跳失率
      df_2("count_at") //打开次数
    ).distinct().na.fill(0)

    //rs.show(1000)


    var statement: Statement = null
    //逐条写入 数据库
    result.foreachPartition((rows: Iterator[Row]) => {
//      val conn = JdbcUtil.getConn()
//      val statement = conn.createStatement
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_daily_entrance_page (app_key,day,entry_page_count,one_page_count,page_count,visitor_count,open_count," +
        "total_time,avg_stay_time,bounce_rate,update_at)" + s"values(?,?,?,?,?,?,?,?,?,?,?) " +
        s"ON DUPLICATE KEY UPDATE one_page_count=?, entry_page_count=?,visitor_count=?,page_count=?,total_time=?,avg_stay_time=?" +
        s",bounce_rate=?,open_count=?,update_at=?"

      rows.foreach(r => {

        val app_key = r.get(0)
        val day = yesterday
        val ifp_count = r.get(1) //入口页次数
        val visitor_count = r.get(2) //访问人数
        val page_count = r.get(3) //访问次数
        val total_time = r.get(4) //总停留时长
        val avg_stay_time = r.get(5) //次均停留时长
        val one_page_count = r.get(6) //跳出页个数
        val bounce_rate = r.get(7) //跳出率
        val count_at = r.get(8) //总打开次数
        val update_at = TimeUtil.nowInt()

        params.+=(Array[Any](app_key, day, ifp_count, one_page_count, page_count, visitor_count, count_at, total_time, avg_stay_time, bounce_rate, update_at, one_page_count, ifp_count, visitor_count, page_count, total_time, avg_stay_time, bounce_rate, count_at, update_at))
      })
      //      try {
      JdbcUtil.doBatch(sqlText, params) //批量入库
      //      }
      //      finally {
      //        statement.close() //关闭statement
      //        conn.close() //关闭数据库连接
      //      }
    })
    sparkSession.stop()
  }
}