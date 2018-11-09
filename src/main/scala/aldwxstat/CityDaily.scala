//==========================================================
/*gcs:
*这个文件对应哪一个功能模块？？？？
*/
package aldwxstat

import java.sql.Timestamp

import aldwxconfig.ConfigurationUtil
import aldwxutils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * 按城市统计每个城市的新访问用户数, 访问人数, 打开次数, 访问次数
  */
object CityDaily {
  def main(args: Array[String]): Unit = {

    //gcs:日志过滤
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "weilongsheng")


    val sparkseesion = SparkSession.builder()
      .appName(this.getClass.getName)
      //.config("spark.sql.shuffle.partitions", 100)
      .getOrCreate()


    //获取昨天时间yyyyMMdd
    val yesterday = TimeUtil.processArgs(args)
    println(yesterday)
    val tmp_args = "city"
    val dateTime = TimeUtil.getTimestamp(yesterday)
    val df = ArgsTool.getLogs(args, sparkseesion, ConfigurationUtil.getProperty("tongji.parquet"))
    df.createOrReplaceTempView(s"phone_brand_$tmp_args")



    val UpDataTime = new Timestamp(System.currentTimeMillis())

    //新访客(new_comer_count)
    val newnum = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,COUNT(DISTINCT uu) new_comer_count FROM phone_brand_$tmp_args WHERE ev = 'app' AND ifo='true' GROUP BY ak,${tmp_args} ")
    //访问人数(visitor_count)
    val visnum = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,COUNT(DISTINCT uu) visitor_count FROM phone_brand_$tmp_args WHERE ev='app' GROUP BY ak,${tmp_args} ")
    //打开次数(open_count)
    val opennum = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,COUNT(DISTINCT at) open_count FROM phone_brand_$tmp_args WHERE ev='app' GROUP BY ak,${tmp_args}")
    //页面总访问量
    val totalpage = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,COUNT(pp) total_page_count FROM phone_brand_$tmp_args WHERE ev='page' GROUP BY ak,${tmp_args}")
    //    totalpage.show()
    //总停留时长
    val totaltimes = sparkseesion.sql(s"SELECT ak app_key,${tmp_args} tmp_sum,sum(x) total_stay_time FROM (SELECT ak,${tmp_args},cast(max(st)/1000 as int)-cast(min(st)/1000 as int) x FROM phone_brand_$tmp_args WHERE ev='page' GROUP BY ak,at,${tmp_args}) group by ak,${tmp_args}")
    //创建临时表
    newnum.createTempView(s"newnum1_$tmp_args")
    visnum.createTempView(s"visnum1_$tmp_args")
    opennum.createTempView(s"opennum1_$tmp_args")
    totalpage.createTempView(s"totalpage1_$tmp_args")
    totaltimes.createTempView(s"totaltimes1_$tmp_args")
    //次均
    val pl = sparkseesion.sql(s"SELECT a.app_key app_key,a.tmp_sum tmp_sum,cast(b.total_stay_time/a.visitor_count as int) avg_stay_time FROM visnum1_$tmp_args a left join totaltimes1_$tmp_args b on a.app_key=b.app_key and a.tmp_sum = b.tmp_sum")
    pl.createTempView(s"pl1_$tmp_args")

    //访问一次页面的次数
    val times_one2 = sparkseesion.sql(s"select tmp.ak app_key,tmp.tmp_sum_one tmp_sum,sum(tmp.cp) one_cp from (SELECT ak,at ,${tmp_args} tmp_sum_one,COUNT(pp) cp FROM phone_brand_$tmp_args where ev = 'page' GROUP BY ak,at,pp,${tmp_args}) tmp group by tmp.ak,tmp.at,tmp.tmp_sum_one")

    times_one2.createTempView(s"times_one2_$tmp_args")
    val times_one3 = sparkseesion.sql(s"select app_key,tmp_sum,sum(one_cp) one_page_count from times_one2_$tmp_args where one_cp = 1 group by app_key,tmp_sum")
    times_one3.createTempView(s"times_one_$tmp_args")
    //页面跳出率
    val bounce = sparkseesion.sql(s"select a.app_key app_key,a.tmp_sum tmp_sum,cast(b.one_page_count/a.total_page_count as float) bounce_rate from totalpage1_$tmp_args a left join times_one_$tmp_args b on a.app_key = b.app_key and a.tmp_sum = b.tmp_sum")
    bounce.createTempView(s"bounce1_$tmp_args")
    val result = sparkseesion.sql(s"SELECT a.app_key,a.tmp_sum city,a.total_page_count page_count,b.visitor_count,c.open_count,d.total_stay_time,e.avg_stay_time secondary_stay_time,f.new_comer_count new_user_count,g.one_page_count,h.bounce_rate from totalpage1_$tmp_args a left join visnum1_$tmp_args b on a.app_key = b.app_key and a.tmp_sum = b.tmp_sum left join opennum1_$tmp_args c on b.app_key = c.app_key and b.tmp_sum = c.tmp_sum left join totaltimes1_$tmp_args d on c.app_key = d.app_key and c.tmp_sum = d.tmp_sum left join pl1_$tmp_args e on d.app_key = e.app_key and d.tmp_sum = e.tmp_sum left join newnum1_$tmp_args f on e.app_key = f.app_key and e.tmp_sum = f.tmp_sum left join times_one_$tmp_args g on g.app_key = f.app_key and g.tmp_sum = f.tmp_sum left join bounce1_$tmp_args h on g.app_key = h.app_key and g.tmp_sum = h.tmp_sum").distinct().na.fill(0)

    result.show()
    //    result.foreachPartition((rows: Iterator[Row]) => {
    //      val conn = MySqlPool.getJdbcConn()
    //      val statement = conn.createStatement()
    //      try {
    //        conn.setAutoCommit(false)
    //        rows.foreach(r => {
    //          val day = dateTime
    //          val update_at = UpDataTime
    //          val app_key = r(0)
    //          val type_values = r(1)
    //          val page_count = r(2)
    //          val visitor_count = r(3)
    //          val open_count = r(4)
    //          val total_stay_time = r(5)
    //          val secondary_stay_time = r(6)
    //          val new_user_count = r(7)
    //          val one_page_count = r(8)
    //          val bounce_rate = r(9)
    //
    //          val sql = s"insert into aldstat_city_statistics (app_key,day,new_user_count, visitor_count,open_count,page_count,secondary_stay_time,total_stay_time,bounce_rate,one_page_count,update_at,city)" +
    //            s"values ('${app_key}','${day}','${new_user_count}','${visitor_count}','${open_count}','${page_count}','${secondary_stay_time}', '${total_stay_time}','${bounce_rate}','${one_page_count}','${update_at}','${type_values}') ON " +
    //            s"DUPLICATE KEY UPDATE new_user_count='${new_user_count}', visitor_count='${visitor_count}', open_count='${open_count}',page_count='${page_count}', secondary_stay_time='${secondary_stay_time}',total_stay_time='${total_stay_time}', bounce_rate='${bounce_rate}',one_page_count='${one_page_count}',update_at='${update_at}'"
    //          statement.addBatch(sql)
    //        })
    //        statement.executeBatch
    //        conn.commit()
    //      } catch {
    //        case e: Exception => e.printStackTrace()
    //          conn.close()
    //      }
    //    })

    result.foreachPartition((rows: Iterator[Row]) => {
//      val conn = JdbcUtil.getConn()
//      val statement = conn.createStatement
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = s"insert into aldstat_city_statistics (app_key,day,new_user_count, visitor_count,open_count,page_count,secondary_stay_time,total_stay_time,bounce_rate,one_page_count,update_at,city)" +
        s"values (?,?,?,?,?,?,?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE new_user_count=?, visitor_count=?, open_count=?,page_count=?, " +
        s"secondary_stay_time=?,total_stay_time=?, bounce_rate=?,one_page_count=?,update_at=?"

      rows.foreach(r => {
        val day = dateTime
        val update_at = UpDataTime
        val app_key = r(0)
        val type_values = r(1)
        val page_count = r(2)
        val visitor_count = r(3)
        val open_count = r(4)
        val total_stay_time = r(5)
        val secondary_stay_time = r(6)
        val new_user_count = r(7)
        val one_page_count = r(8)
        val bounce_rate = r(9)

        params.+=(Array[Any](app_key, day, new_user_count, visitor_count, open_count, page_count, secondary_stay_time, total_stay_time, bounce_rate, one_page_count, update_at, type_values,
          new_user_count, visitor_count, open_count, page_count, secondary_stay_time, total_stay_time, bounce_rate, one_page_count, update_at))

      })
      //      try {
      JdbcUtil.doBatch(sqlText, params) //批量入库
      //      }
      //      finally {
      //        statement.close() //关闭statement
      //        conn.close() //关闭数据库连接
      //      }
    })
    sparkseesion.close()
  }
}
