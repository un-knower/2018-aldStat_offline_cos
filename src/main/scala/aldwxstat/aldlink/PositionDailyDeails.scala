package aldwxstat.aldlink

import java.sql.Timestamp
import java.text.MessageFormat

import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangtaiyang on 2017/12/14. 
  */
class PositionDailyDeails(spark: SparkSession) extends LinkAnalysis {
  //新用户数
  def newUser(): Unit = {
    //spark.sql("SELECT ak,uu FROM visitora_app WHERE ifo='true' GROUP BY ak,uu").createOrReplaceTempView("nu_app_daily")
    //spark.sql("SELECT ak,uu FROM visitora WHERE ev = 'page' and ag_ald_position_id!=null and ag_ald_position_id!=null and ag_ald_position_id!=null GROUP BY ak,uu")
    spark.sql("SELECT ak,ag_ald_position_id,uu FROM visitora_page GROUP BY ak,ag_ald_position_id,uu")
      .createOrReplaceTempView("nu_page_daily")
    spark.sql(
      """select ak app_key,ag_ald_position_id,count(uu) new_comer_count
        |from
        |(select page.ak ak,page.ag_ald_position_id ag_ald_position_id,page.uu from nu_app_daily app inner join nu_page_daily page on page.ak=app.ak and page.uu=app.uu)
        |group by ak,ag_ald_position_id
      """.stripMargin).createOrReplaceTempView("new_user_daily")
    //spark.sql("select * from new_user_daily").show()
  }

  //访问人数(visitor_count)
  def visitorCount(): Unit = {
    spark.sql("SELECT ak app_key,ag_ald_position_id,COUNT(DISTINCT uu) visitor_count FROM visitora_page GROUP BY ak,ag_ald_position_id")
      .createOrReplaceTempView("visitor_count")
    //spark.sql("select * from visitor_count").show()
  }

  //打开次数(open_count)
  def openCount(): Unit = {
    spark.sql("SELECT ak app_key,ag_ald_position_id,COUNT(DISTINCT at) open_count FROM visitora_page GROUP BY ak,ag_ald_position_id")
      .createOrReplaceTempView("open_count")
    //spark.sql("select * from open_count").show()
  }

  //访问次数，页面总访问量
  def totalPageCount(): Unit = {
    spark.sql("SELECT ak app_key,ag_ald_position_id,COUNT(pp) total_page_count FROM visitora_page GROUP BY ak,ag_ald_position_id")
      .createOrReplaceTempView("total_page_count")
    //spark.sql("select * from total_page_count").show()
  }

  //总停留时长
  def totalStayTime(): Unit = {
    //    spark.sql(
    //      """
    //        |SELECT ak app_key,ag_ald_position_id,sum(total_dr) total_stay_time
    //        |FROM (SELECT ak,ag_ald_position_id,sum(dr) total_dr FROM visitora_page GROUP BY ak,ag_ald_position_id,at)
    //        |group by ak,ag_ald_position_id
    //      """.stripMargin).createOrReplaceTempView("total_stay_time")
    spark.sql(
      """
        |select app_key,ag_ald_position_id,sum(dr) total_stay_time from
        |(
        |select page.ak app_key,page.ag_ald_position_id ag_ald_position_id,page.dr dr
        |from visitora_page page
        |left join app_tmp app
        |on page.ak=app.ak and page.at=app.at
        |)
        |group by app_key,ag_ald_position_id
      """.stripMargin).createOrReplaceTempView("total_stay_time")
    //spark.sql("select * from total_stay_time").show()
  }

  //次均停留时长
  def secondaryAvgStayTime(): Unit = {
    spark.sql(
      """
        |SELECT oc.app_key,oc.ag_ald_position_id,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
        |FROM open_count oc
        |left join total_stay_time tst
        |on oc.app_key=tst.app_key and oc.ag_ald_position_id=tst.ag_ald_position_id
      """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
    //spark.sql("select * from secondary_avg_stay_time").show()
  }

  //跳出页个数
  def visitPageOnce(): Unit = {
    spark.sql(//|ak，at，所有page的访问次数=1|
      """
        |select tmp.ak app_key,tmp.ag_ald_position_id ag_ald_position_id,sum(tmp.cp) once_page
        |from (SELECT ak,ag_ald_position_id,at,COUNT(pp) cp FROM visitora_page GROUP BY ak,ag_ald_position_id,at,pp) tmp
        |group by tmp.ak,tmp.ag_ald_position_id,tmp.at
      """.stripMargin).createOrReplaceTempView("visit_page_sum_once")
    spark.sql(//|ak，at，所有page的访问次数=1|
      s"""
         |select app_key,ag_ald_position_id,sum(once_page) one_page_count
         |from visit_page_sum_once
         |where once_page=1
         |group by app_key,ag_ald_position_id
       """.stripMargin).createOrReplaceTempView("visit_page_once")
    //spark.sql("select * from visit_page_once").show()
  }

  //页面跳出率,每个ak的跳出页个数/总的页面访问量
  def bounceRate(): Unit = {
    spark.sql(
      """
        |select tpc.app_key app_key,tpc.ag_ald_position_id ag_ald_position_id,cast(vpo.one_page_count/tpc.total_page_count as float) bounce_rate
        |from total_page_count tpc
        |left join visit_page_once vpo
        |on tpc.app_key = vpo.app_key and tpc.ag_ald_position_id=vpo.ag_ald_position_id
      """.stripMargin).createOrReplaceTempView("bounce_rate")
    //spark.sql("select * from bounce_rate").show()
  }


  //外链每日详情入库
  def insert2db(): Unit = {
    val day = ArgsTool.day

    val positionDailyDetail = spark.sql(MessageFormat.format(
      """
        |select vc.app_key,vc.ag_ald_position_id,vc.visitor_count,oc.open_count,tpc.total_page_count,nu.new_comer_count,
        |tst.total_stay_time,sast.secondary_avg_stay_time,vpo.one_page_count,bounce.bounce_rate
        |from visitor_count vc
        |left join open_count oc
        |on vc.app_key=oc.app_key and vc.ag_ald_position_id=oc.ag_ald_position_id
        |left join total_page_count tpc
        |on vc.app_key=tpc.app_key and vc.ag_ald_position_id=tpc.ag_ald_position_id
        |left join new_user_daily nu
        |on vc.app_key=nu.app_key and vc.ag_ald_position_id=nu.ag_ald_position_id
        |left join total_stay_time tst
        |on vc.app_key=tst.app_key and vc.ag_ald_position_id=tst.ag_ald_position_id
        |left join secondary_avg_stay_time sast
        |on vc.app_key=sast.app_key and vc.ag_ald_position_id=sast.ag_ald_position_id
        |left join visit_page_once vpo
        |on vc.app_key=vpo.app_key and vc.ag_ald_position_id=vpo.ag_ald_position_id
        |left join bounce_rate bounce
        |on vc.app_key=bounce.app_key and vc.ag_ald_position_id=bounce.ag_ald_position_id
      """.stripMargin, day, new Timestamp(System.currentTimeMillis())))

    positionDailyDetail.na.fill("0").na.fill(0).foreachPartition(rows => {
      val params = new ArrayBuffer[Array[Any]]()
      val sqlText =
        """
          |insert into aldstat_daily_position(app_key,day,position_id,position_visitor_count,position_open_count,position_page_count,
          |position_newer_for_app,total_stay_time,secondary_stay_time,one_page_count,bounce_rate,update_at)
          |values(?,?,?,?,?,?,?,?,?,?,?,?)
          |on duplicate key update
          |position_visitor_count=?,position_open_count=?,position_page_count=?,position_newer_for_app=?,total_stay_time=?,
          | secondary_stay_time=?,one_page_count=?,bounce_rate=?,update_at=?
        """.stripMargin


      val update_at = new Timestamp(System.currentTimeMillis()).toString
      rows.foreach(row => {
        val app_key = row.get(0).toString
        val ag_ald_position_id = row.get(1).toString
        val visitor_count = row.get(2).toString
        val open_count = row.get(3).toString
        val total_page_count = row.get(4).toString
        val new_user_daily = row.get(5).toString
        val total_stay_time = row.get(6).toString
        val secondary_avg_stay_time = row.get(7).toString
        val visit_page_once = row.get(8).toString
        val bounce_rate = row.get(9).toString

        params.+=(Array[Any](app_key, day, ag_ald_position_id, visitor_count, open_count, total_page_count,
          new_user_daily, total_stay_time, secondary_avg_stay_time, visit_page_once, bounce_rate, update_at,
          visitor_count, open_count, total_page_count, new_user_daily, total_stay_time, secondary_avg_stay_time
          , visit_page_once, bounce_rate, update_at
        ))

      })
      JdbcUtil.doBatch(sqlText, params) // 批量入库
    })
    //spark.close()
  }
}
