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
  * 详情表
  */
object EntrancePageDaily {
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
        df("pp"),
        df("uu"),
        df("at")
      ).groupBy("ak", "pp")
      .agg(
        countDistinct("uu") as "count_uu", //访问人数
        count("pp") as "count_pp" //入口页次数
      ).distinct()

    //总的打开次数
    sparkSession.sql("SELECT ak,pp,COUNT(DISTINCT at) open FROM jumpDemo where ifp='true' GROUP BY ak,pp").createTempView("open")
    //总的访问次数
    sparkSession.sql("SELECT ak,pp,COUNT(pp) open FROM jumpDemo GROUP BY ak,pp").createTempView("view")
    //总的停留时长
    sparkSession.sql("SELECT ak,pp,sum(time) total_time FROM (SELECT ak,pp,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) time FROM jumpDemo GROUP BY ak,pp,at) group by ak,pp").createTempView("time")
    //统计只有一个访问页（跳失的入口页）
    sparkSession.sql("select t.ak ak, t.pp pp,sum(t.aat) exit from (SELECT ak, at ,COUNT(at) aat,pp FROM jumpDemo where ifp='true' GROUP BY ak,at,pp having count(pp)=1) t group by t.ak,t.pp").createTempView("one_page")
    //次均停留时长   受访页的访问次数
    val staytime = sparkSession.sql("select o.ak,o.pp,t.total_time,cast(t.total_time/o.open as float)as avg_stay_time,o.open as count_at from open o left join time t on o.ak=t.ak and o.pp=t.pp")
    //跳失率
    val jump = sparkSession.sql("select v.ak,v.pp,o.exit,cast(o.exit/v.open as float) as bounce_rate,v.open view_open from view v left join one_page o on v.ak=o.ak and v.pp=o.pp")


    val df_2 = staytime.join(jump, staytime("ak") === jump("ak") &&
      staytime("pp") === jump("pp")
    ).select(
      staytime("ak"),
      staytime("pp"),
      staytime("total_time"),
      staytime("avg_stay_time"),
      jump("exit"),
      jump("bounce_rate"),
      jump("view_open"),
      staytime("count_at")
    ).distinct()

    val rs = df_1.join(df_2, df_1("ak") === df_2("ak") &&
      df_1("pp") === df_2("pp")
    ).select(
      df_1("ak"),
      df_1("pp"),
      df_1("count_pp"),
      df_1("count_uu"),
      df_2("view_open"),
      df_2("total_time"),
      df_2("avg_stay_time"),
      df_2("exit"),
      df_2("bounce_rate"),
      df_2("count_at")
    ).distinct().na.fill(0)


    var statement: Statement = null
    rs.foreachPartition((rows: Iterator[Row]) => {
      //连接池
//      val conn = JdbcUtil.getConn()
//      val statement = conn.createStatement
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_entrance_page (app_key,day,page_path,entry_page_count,one_page_count,page_count,visitor_count,open_count,total_time,avg_stay_time,bounce_rate,update_at)" + s"values(?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE entry_page_count=?,one_page_count=?, page_count=?,visitor_count=?,open_count=?,total_time=?,avg_stay_time=?,bounce_rate=?,update_at=?"

      rows.foreach(r => {

        val app_key = r.get(0)
        val day = yesterday
        val page_path = r.get(1)
        val entry_page_count = r.get(2) //入口页次数
        val visitor_count = r.get(3) //访问人数
        val page_count = r.get(4) //访问次数
        val total_time = r.get(5) //总停留时长
        val avg_stay_time = r.get(6) //次均停留时长
        val one_page_count = r.get(7) //入口页个数是一个
        val bounce_rate = r.get(8) //跳出率
        val open_count = r.get(9) //总打开次数
        val update_at = TimeUtil.nowInt()
        params.+=(Array[Any](app_key, day, page_path, entry_page_count, one_page_count, page_count, visitor_count, open_count, total_time, avg_stay_time
          , bounce_rate, update_at, entry_page_count, one_page_count, page_count, visitor_count, open_count, total_time, avg_stay_time, bounce_rate, update_at))

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