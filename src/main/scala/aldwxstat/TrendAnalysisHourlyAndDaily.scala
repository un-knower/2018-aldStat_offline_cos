package aldwxstat

import java.sql.Timestamp

import aldwxconfig.ConfigurationUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import aldwxutils._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ald on 2017/6/22.
  * 趋势分析今天和每小时的
  */
object TrendAnalysisHourlyAndDaily {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkseesion = SparkSession.builder()
      .appName(this.getClass.getName)
      //        .master("local")
      //      .config("spark.sql.shuffle.partitions", 12)
      .getOrCreate()

    //获取昨天时间yyyyMMdd
    val yesterday = TimeUtil.processArgs(args)
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val dateTime = TimeUtil.getTimestamp(yesterday)
    val df = ArgsTool.getLogs(args, sparkseesion, ConfigurationUtil.getProperty("tongji.parquet"))

    //==========================================================1
    /*
    *gcs:
    *将读取到的log日志转换为一个视图
    */
    df.createTempView("visitora")

    //==========================================================2
    /*
    *gcs:
    *用来计算每一小时的总人数
    */
    //新访客(new_comer_count)
    val newnum = sparkseesion.sql("SELECT ak app_key,hour,COUNT(DISTINCT uu) new_comer_count FROM visitora WHERE ev = 'app' AND ifo='true' GROUP BY ak,hour ")
    //    newnum.show()
    //访问人数(visitor_count)
    val visnum = sparkseesion.sql("SELECT ak app_key,hour,COUNT(DISTINCT uu) visitor_count FROM visitora WHERE ev='app' GROUP BY ak,hour ")
    //    visnum.show()
    //    //打开次数(open_count)
    val opennum = sparkseesion.sql("SELECT ak app_key,hour,COUNT(DISTINCT at) open_count FROM visitora WHERE ev='app' GROUP BY ak,hour")
    //    opennum.show()
    //页面总访问量
    val totalpage = sparkseesion.sql("SELECT ak app_key,hour,COUNT(pp) total_page_count FROM visitora WHERE ev='page' GROUP BY ak,hour")
    //    totalpage.show()
    //总停留时长
    val totaltimes = sparkseesion.sql("SELECT ak app_key,hour,sum(x) total_stay_time FROM (SELECT ak,hour,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) x FROM visitora WHERE ev='page' GROUP BY ak,at,hour) group by ak,hour")
    //创建临时表
    newnum.createTempView("newnum1")
    visnum.createTempView("visnum1")
    opennum.createTempView("opennum1")
    totalpage.createTempView("totalpage1")
    totaltimes.createTempView("totaltimes1")
    //次均
    val pl = sparkseesion.sql("SELECT a.app_key,a.hour,cast(b.total_stay_time/a.visitor_count as int) avg_stay_time FROM visnum1 a left join totaltimes1 b on a.app_key=b.app_key and a.hour = b.hour")
    pl.createTempView("pl1")
    //    pl.show()
    //    //人均
    val ol = sparkseesion.sql("SELECT a.app_key,a.hour,cast(b.total_stay_time/a.open_count as int) secondary_avg_stay_time FROM opennum1 a left join totaltimes1 b on a.app_key=b.app_key and a.hour = b.hour")
    ol.createTempView("ol1")
    // ol.show()
    //创建mysql连接
    //    val url = dbconf.url
    //    val prop =new Properties()
    //
    //    prop.put("driver",dbconf.driver)
    //    prop.setProperty("user",dbconf.user)
    //    prop.setProperty("password",dbconf.password)
    if (df.columns.contains("ct") && df.columns.contains("ct_errMsg")) {
      val pageuu = sparkseesion.sql("SELECT ak app_key,hour,path page_uri,uu sharer_uuid,COUNT(path) share_count FROM visitora WHERE ev='event'  and tp='ald_share_status' and (ct!= 'fail' or ct_errMsg='shareAppMessage:ok') and path IS not NULL GROUP BY ak,path,uu,hour")
      //      pageuu.show()
      pageuu.createTempView("share1")

    } else if (df.columns.contains("ct_errMsg")) {
      val pageuu = sparkseesion.sql("SELECT ak app_key,hour,path page_uri,uu sharer_uuid,COUNT(path) share_count FROM visitora WHERE ev='event' and ct_errMsg='shareAppMessage:ok' and tp='ald_share_status' and path IS not NULL GROUP BY ak,path,uu,hour")
      //      pageuu.show()
      pageuu.createTempView("share1")

    } else if (df.columns.contains("ct")) {
      val pageuu = sparkseesion.sql("SELECT ak app_key,hour,path page_uri,uu sharer_uuid,COUNT(path) share_count FROM visitora WHERE ev='event' and  ct!= 'fail' and tp='ald_share_status' and tp='ald_share_status' and path IS not NULL GROUP BY ak,path,uu,hour")
      //      pageuu.show()
      pageuu.createTempView("share1")
    }
    val sharecou = sparkseesion.sql(s"SELECT app_key,hour,sum(share_count) daily_share_count FROM share1 GROUP BY app_key,hour")
    //    sharecou.show()
    //访问一次页面的次数
    //    val times_one = sparkseesion.sql(s"select tmp.ak app_key,tmp.hour hour,sum(tmp.aat) one_page_count from (SELECT ak,at ,COUNT(at) aat FROM visitora where ev = 'page' GROUP BY ak,at,pp,hour having count(pp)=1) tmp group by tmp.ak,tmp.hour").distinct()
    val times_one2 = sparkseesion.sql(s"select tmp.ak app_key,tmp.hour hour,sum(tmp.cp) one_cp from (SELECT ak,at ,hour,COUNT(pp) cp FROM visitora where ev = 'page' GROUP BY ak,at,pp,hour) tmp group by tmp.ak,tmp.at,tmp.hour")
    //    times_one2.show()
    times_one2.createTempView("times_one2")
    val times_one3 = sparkseesion.sql(s"select app_key,hour,sum(one_cp) one_page_count from times_one2 where one_cp = 1 group by app_key,hour")
    times_one3.createTempView("times_one")
    //页面跳出率
    val bounce = sparkseesion.sql("select a.app_key app_key,a.hour hour,cast(b.one_page_count/a.total_page_count as float) bounce_rate from totalpage1 a left join times_one b on a.app_key = b.app_key and a.hour = b.hour")
    //    bounce.show()
    sharecou.createTempView("sharecou1")
    bounce.createTempView("bounce1")
    //    val result = sparkseesion.sql("SELECT "+yesterday+" day,UNIX_TIMESTAMP(now()) update_at,a.app_key,a.new_comer_count,b.visitor_count,c.open_count,d.total_page_count,e.total_stay_time,f.avg_stay_time,i.secondary_avg_stay_time,j.daily_share_count,k.bounce_rate,a.hour FROM newnum1 a left join visnum1 b on a.app_key = b.app_key and a.hour =b.hour left join opennum1 c on b.app_key = c.app_key and b.hour =c.hour left join totalpage1 d on c.app_key = d.app_key and c.hour = d.hour left join totaltimes1 e on d.app_key = e.app_key and d.hour = e.hour left join pl1 f on e.app_key = f.app_key and e.hour = f.hour left join ol1 i on f.app_key = i.app_key and f.hour = i.hour left join sharecou1 j on i.app_key=j.app_key and i.hour = j.hour left join bounce1 k on j.app_key=k.ak and j.hour = k.hour").distinct().na.fill(0)
    val result = sparkseesion.sql(s"SELECT " + yesterday + " day,UNIX_TIMESTAMP(now()) update_at,a.app_key,a.total_page_count,b.visitor_count,c.open_count,d.total_stay_time,e.avg_stay_time,f.new_comer_count,g.one_page_count,h.bounce_rate,m.daily_share_count,aaa.secondary_avg_stay_time,a.hour from totalpage1 a left join visnum1 b on a.app_key = b.app_key and a.hour = b.hour left join opennum1 c on b.app_key = c.app_key and b.hour = c.hour left join totaltimes1 d on c.app_key = d.app_key and c.hour = d.hour left join pl1 e on d.app_key = e.app_key and d.hour= e.hour left join ol1 aaa on e.app_key = aaa.app_key and e.hour = aaa.hour left join newnum1 f on e.app_key = f.app_key and e.hour = f.hour left join times_one g on g.app_key = f.app_key and g.hour = f.hour left join bounce1 h on f.app_key = h.app_key and f.hour = h.hour left join sharecou1 m  on h.app_key = m.app_key and h.hour = m.hour ").distinct().na.fill(0).filter("hour != 'null'")
    //    left join ol1 n on m.app_key = n.app_key and m.hour = n.hour
    //    n.secondary_avg_stay_time,
    //    val result = sparkseesion.sql("")?
    // result.show()
    result.foreachPartition((rows: Iterator[Row]) => {
      //      val conn =MySqlPool.getJdbcConn()
      //      val statement = conn.createStatement()
      //      try {
      //        conn.setAutoCommit(false)
      //      rows.foreach(r=>{
      //        val day=dateTime
      //        val update_at=UpDataTime
      //        val app_key=r(2)
      //        val total_page_count=r(3)
      //        val visitor_count=r(4)
      //        val open_count=r(5)
      //        val total_stay_time=r(6)
      //        val avg_stay_time=r(7)
      //        val new_comer_count=r(8)
      //        val one_page_count=r(9)
      //        val bounce_rate=r(10)
      //        val daily_share_count=r(11)
      //        val secondary_avg_stay_time = r(12)
      //        val hour = r(13)
      //          val sql = s"insert into aldstat_hourly_trend_analysis (app_key,day,new_comer_count, visitor_count,open_count,total_page_count,avg_stay_time,secondary_avg_stay_time,total_stay_time,daily_share_count,bounce_rate,update_at,hour,one_page_count)" +
      //            s"values ('${app_key}','${day}','${new_comer_count}','${visitor_count}','${open_count}','${total_page_count}','${avg_stay_time}', '${secondary_avg_stay_time}','${total_stay_time}','${daily_share_count}','${bounce_rate}','${update_at}',${hour},${one_page_count}) ON " +
      //            s"DUPLICATE KEY UPDATE new_comer_count='${new_comer_count}', visitor_count='${visitor_count}', open_count='${open_count}',total_page_count='${total_page_count}', avg_stay_time='${avg_stay_time}', secondary_avg_stay_time='${secondary_avg_stay_time}',total_stay_time='${total_stay_time}', daily_share_count='${daily_share_count}', bounce_rate='${bounce_rate}',update_at='${update_at}',one_page_count='${one_page_count}'"
      //          statement.addBatch(sql)
      //        })
      //        statement.executeBatch
      //        conn.commit()
      //      }catch {
      //        case e: Exception => e.printStackTrace()
      //          conn.close()
      //      }
      //      val conn = JdbcUtil.getConn()
      //      val statement = conn.createStatement
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = s"insert into aldstat_hourly_trend_analysis (app_key,day,new_comer_count, visitor_count,open_count,total_page_count,avg_stay_time,secondary_avg_stay_time,total_stay_time,daily_share_count,bounce_rate,update_at,hour,one_page_count)" +
        s"values (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE new_comer_count=?, visitor_count=?, open_count=?,total_page_count=?, avg_stay_time=?, " +
        s"secondary_avg_stay_time=?,total_stay_time=?, daily_share_count=?, bounce_rate=?,update_at=?,one_page_count=?"

      rows.foreach(r => {
        val day = dateTime
        val update_at = UpDataTime
        val app_key = r.get(2)
        val total_page_count = r.get(3)
        val visitor_count = r.get(4)
        val open_count = r.get(5)
        val total_stay_time = r.get(6)
        val avg_stay_time = r.get(7)
        val new_comer_count = r.get(8)
        val one_page_count = r.get(9)
        val bounce_rate = r.get(10)
        val daily_share_count = r.get(11)
        val secondary_avg_stay_time = r.get(12)
        val hour = r.get(13)

        params.+=(Array[Any](app_key, day, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, secondary_avg_stay_time, total_stay_time,
          daily_share_count, bounce_rate, update_at, hour, one_page_count, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, secondary_avg_stay_time
          , total_stay_time, daily_share_count, bounce_rate, update_at, one_page_count))
      })
      //      try {
      JdbcUtil.doBatch(sqlText, params) //批量入库
      //      }
      //      finally {
      //        statement.close() //关闭statement
      //        conn.close() //关闭数据库连接
      //      }
    })


    //---------------------------------每天的趋势分析------------------------------


    val newnum_day = sparkseesion.sql("SELECT ak app_key,COUNT(DISTINCT uu) new_comer_count FROM visitora WHERE ev = 'app' AND ifo='true' GROUP BY ak ")
    //访问人数(visitor_count)
    val visnum_day = sparkseesion.sql("SELECT ak app_key,COUNT(DISTINCT uu) visitor_count FROM visitora WHERE ev='app' GROUP BY ak ")
    //    //打开次数(open_count)
    val opennum_day = sparkseesion.sql("SELECT ak app_key,COUNT(DISTINCT at) open_count FROM visitora WHERE ev='app' GROUP BY ak")
    //页面总访问量
    val totalpage_day = sparkseesion.sql("SELECT ak app_key,COUNT(pp) total_page_count FROM visitora WHERE ev='page' GROUP BY ak")
    //    totalpage.show()
    //总停留时长
    val totaltimes_day = sparkseesion.sql("SELECT ak app_key,sum(x) total_stay_time FROM (SELECT ak,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) x FROM visitora WHERE ev='page' GROUP BY ak,at) group by ak")
    //创建临时表
    newnum_day.createTempView("newnum1_day")
    visnum_day.createTempView("visnum1_day")
    opennum_day.createTempView("opennum1_day")
    totalpage_day.createTempView("totalpage1_day")
    totaltimes_day.createTempView("totaltimes1_day")
    //人均
    val pl_day = sparkseesion.sql("SELECT a.app_key,cast(b.total_stay_time/a.visitor_count as int) avg_stay_time FROM visnum1_day a left join totaltimes1_day b on a.app_key=b.app_key")
    pl_day.createTempView("pl1_day")
    //    //次均
    val ol_day = sparkseesion.sql("SELECT a.app_key,cast(b.total_stay_time/a.open_count as int) secondary_avg_stay_time FROM opennum1_day a left join totaltimes1_day b on a.app_key=b.app_key")
    ol_day.createTempView("ol1_day")
    // ol_day.show()
    //创建mysql连接
    //    val url = dbconf.url
    //    val prop =new Properties()
    //
    //    prop.put("driver",dbconf.driver)
    //    prop.setProperty("user",dbconf.user)
    //    prop.setProperty("password",dbconf.password)
    if (df.columns.contains("ct") && df.columns.contains("ct_errMsg")) {
      val pageuu = sparkseesion.sql("SELECT ak app_key,path page_uri,uu sharer_uuid,COUNT(path) share_count FROM visitora WHERE ev='event'  and tp='ald_share_status' and (ct!= 'fail' or ct_errMsg='shareAppMessage:ok') and path IS not NULL GROUP BY ak,path,uu")
      //      pageuu.show()
      pageuu.createTempView("share1_day")

    } else if (df.columns.contains("ct_errMsg")) {
      val pageuu = sparkseesion.sql("SELECT ak app_key,path page_uri,uu sharer_uuid,COUNT(path) share_count FROM visitora WHERE ev='event' and ct_errMsg='shareAppMessage:ok' and tp='ald_share_status' and path IS not NULL GROUP BY ak,path,uu")
      //      pageuu.show()
      pageuu.createTempView("share1_day")

    } else if (df.columns.contains("ct")) {
      val pageuu = sparkseesion.sql("SELECT ak app_key,path page_uri,uu sharer_uuid,COUNT(path) share_count FROM visitora WHERE ev='event' and  ct!= 'fail' and tp='ald_share_status' and tp='ald_share_status' and path IS not NULL GROUP BY ak,path,uu")
      //      pageuu.show()
      pageuu.createTempView("share1_day")
    }
    val sharecou_day = sparkseesion.sql(s"SELECT app_key,sum(share_count) daily_share_count FROM share1_day GROUP BY app_key")
    //    sharecou.show()
    //访问一次页面的次数
    //    val times_one = sparkseesion.sql(s"select tmp.ak app_key,sum(tmp.aat) one_page_count from (SELECT ak,at ,COUNT(at) aat FROM visitora where ev = 'page' GROUP BY ak,at,pp having count(pp)=1) tmp group by tmp.ak").distinct()
    val times_one2_day = sparkseesion.sql(s"select tmp.ak app_key,sum(tmp.cp) one_cp from (SELECT ak,at ,COUNT(pp) cp FROM visitora where ev = 'page' GROUP BY ak,at,pp) tmp group by tmp.ak,tmp.at")
    //    times_one2.show()
    times_one2_day.createTempView("times_one2_day")
    val times_one3_day = sparkseesion.sql(s"select app_key,sum(one_cp) one_page_count from times_one2_day where one_cp = 1 group by app_key")
    times_one3_day.createTempView("times_one_day")
    //页面跳出率
    val bounce_day = sparkseesion.sql("select a.app_key app_key,cast(b.one_page_count/a.total_page_count as float) bounce_rate from totalpage1_day a left join times_one_day b on a.app_key = b.app_key")
    //    bounce.show()
    sharecou_day.createTempView("sharecou1_day")
    bounce_day.createTempView("bounce1_day")
    //    val result = sparkseesion.sql("SELECT "+yesterday+" day,UNIX_TIMESTAMP(now()) update_at,a.app_key,a.new_comer_count,b.visitor_count,c.open_count,d.total_page_count,e.total_stay_time,f.avg_stay_time,i.secondary_avg_stay_time,j.daily_share_count,k.bounce_rate FROM newnum1 a left join visnum1 b on a.app_key = b.app_key left join opennum1 c on b.app_key = c.app_key left join totalpage1 d on c.app_key = d.app_key left join totaltimes1 e on d.app_key = e.app_key left join pl1 f on e.app_key = f.app_key left join ol1 i on f.app_key = i.app_key left join sharecou1 j on i.app_key=j.app_key left join bounce1 k on j.app_key=k.ak").distinct().na.fill(0)
    val result_day = sparkseesion.sql(s"SELECT " + yesterday + " day,UNIX_TIMESTAMP(now()) update_at,a.app_key,a.total_page_count,b.visitor_count,c.open_count,d.total_stay_time,e.avg_stay_time,f.new_comer_count,g.one_page_count,h.bounce_rate,m.daily_share_count,n.secondary_avg_stay_time from totalpage1_day a left join visnum1_day b on a.app_key = b.app_key left join opennum1_day c on b.app_key = c.app_key left join totaltimes1_day d on c.app_key = d.app_key left join pl1_day e on d.app_key = e.app_key left join newnum1_day f on e.app_key = f.app_key left join times_one_day g on g.app_key = f.app_key left join bounce1_day h on g.app_key = h.app_key left join sharecou1_day m  on h.app_key = m.app_key left join ol1_day n on a.app_key = n.app_key").distinct().na.fill(0)
    //    写入数据库表中
    // result_day.show()
    //    result.write.mode("append").jdbc(url,"aldstat_trend_analysis",prop)

    result_day.foreachPartition((rows: Iterator[Row]) => {

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = s"insert into aldstat_trend_analysis (app_key,day,new_comer_count, visitor_count,open_count,total_page_count,avg_stay_time,secondary_avg_stay_time,total_stay_time,daily_share_count,bounce_rate,update_at,one_page_count)" +
        s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE new_comer_count=?, visitor_count=?, open_count=?,total_page_count=?, avg_stay_time=?, " +
        s"secondary_avg_stay_time=?,total_stay_time=?, daily_share_count=?, bounce_rate=?,update_at=?,one_page_count=?"

      rows.foreach(r => {
        val day = r.get(0)
        val update_at = r.get(1)
        val app_key = r.get(2)
        val total_page_count = r.get(3)
        val visitor_count = r.get(4)
        val open_count = r.get(5)
        val total_stay_time = r.get(6)
        val avg_stay_time = r.get(7)
        val new_comer_count = r.get(8)
        val one_page_count = r.get(9)
        val bounce_rate = r.get(10)
        val daily_share_count = r.get(11)
        val secondary_avg_stay_time = r.get(12)

        params.+=(Array[Any](app_key, day, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, secondary_avg_stay_time, total_stay_time, daily_share_count, bounce_rate, update_at, one_page_count, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, secondary_avg_stay_time, total_stay_time, daily_share_count, bounce_rate, update_at, one_page_count))
      })
      JdbcUtil.doBatch(sqlText, params)
    })

    sparkseesion.close()
  }
}
