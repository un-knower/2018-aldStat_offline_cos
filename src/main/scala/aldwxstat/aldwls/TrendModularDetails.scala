package aldwxstat.aldwls

import org.apache.spark.sql.SparkSession

/**
  * Created by wangtaiyang on 2017/12/14. 
  */
class TrendModularDetails(spark: SparkSession, dimension: String) extends PublicModularAnalysis {
  /**
    * 新用户数
    */
  def newUser(): Unit = {
    if (dimension == "hour") {
      spark.sql(
        s"""SELECT ak app_key,hour,COUNT(DISTINCT uu) new_comer_count
           | FROM app_tmp
           | WHERE ifo='true'
           | GROUP BY ak,hour"""
          .stripMargin)
        .createOrReplaceTempView("new_user_daily")
    } else {
      spark.sql(
        s"""SELECT ak app_key,COUNT(DISTINCT uu) new_comer_count
           | FROM app_tmp
           | WHERE ifo='true'
           | GROUP BY ak"""
          .stripMargin)
        .createOrReplaceTempView("new_user_daily")
    }

  }

  /**
    * 访问人数
    */
  def visitorCount(): Unit = {
    if (dimension == "hour") {
      spark.sql(
        s"""SELECT ak app_key,hour,COUNT(DISTINCT uu) visitor_count
           |FROM app_tmp
           |GROUP BY ak,hour"""
          .stripMargin)
        .createOrReplaceTempView("visitor_count")
    } else {
      spark.sql(
        s"""SELECT ak app_key,COUNT(DISTINCT uu) visitor_count
           |FROM app_tmp
           |GROUP BY ak"""
          .stripMargin)
        .createOrReplaceTempView("visitor_count")
    }

  }

  /**
    * 打开次数
    */
  def openCount(): Unit = {
    if (dimension == "hour") {
      spark.sql(
        s"""SELECT ak app_key,hour,COUNT(DISTINCT at) open_count
           |FROM app_tmp
           |GROUP BY ak,hour"""
          .stripMargin)
        .createOrReplaceTempView("open_count")
    }else {
      spark.sql(
        s"""SELECT ak app_key,COUNT(DISTINCT at) open_count
           |FROM app_tmp
           |GROUP BY ak"""
          .stripMargin)
        .createOrReplaceTempView("open_count")
    }
  }

  /**
    * 访问次数，页面总访问量
    */
  def totalPageCount(): Unit = {
    if (dimension == "hour") {
      spark.sql(
        s"""
           |SELECT ak app_key,hour,COUNT(pp) total_page_count
           |FROM visitor_page
           |GROUP BY hour,ak"""
          .stripMargin)
        .createOrReplaceTempView("total_page_count")
    }else {
      spark.sql(
        s"""
           |SELECT ak app_key,COUNT(pp) total_page_count
           |FROM visitor_page
           |GROUP BY ak"""
          .stripMargin)
        .createOrReplaceTempView("total_page_count")
    }

  }

  /**
    * 总停留时长
    */
  def totalStayTime(): Unit = {
    if (dimension == "hour") {
      spark.sql(
        s"""SELECT ak app_key,hour,sum(time) total_stay_time
           |FROM (
           |SELECT ak,hour,max(dr/1000) time
           |FROM app_tmp GROUP BY ak,at,hour
           |) group by hour,ak"""
          .stripMargin)
        .createOrReplaceTempView("total_stay_time")
    }else {
      spark.sql(
        s"""SELECT ak app_key,sum(time) total_stay_time
           |FROM (
           |SELECT ak,max(dr/1000) time
           |FROM app_tmp GROUP BY ak,at
           |) group by ak"""
          .stripMargin)
        .createOrReplaceTempView("total_stay_time")
    }
  }

  /**
    * 次均停留时长
    */
  def secondaryAvgStayTime(): Unit = {
    if (dimension == "hour") {
      spark.sql(
        """
          |SELECT oc.app_key,oc.hour,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
          |FROM open_count oc
          |left join total_stay_time tst
          |on oc.app_key=tst.app_key and oc.hour=tst.hour
        """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
    }else {
      spark.sql(
        """
          |SELECT oc.app_key,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
          |FROM open_count oc
          |left join total_stay_time tst
          |on oc.app_key=tst.app_key
        """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
    }

  }

  /**
    * 跳出页个数
    */
  def visitPageOnce(): Unit = {
    if (dimension == "hour") {
      spark.sql(//|ak，at，所有page的访问次数=1|
        s"""select tmp.ak app_key,tmp.hour,sum(tmp.cp) one_cp from
           | (SELECT ak,at ,hour,COUNT(pp) cp
           | FROM visitor_page
           | GROUP BY ak,at,pp,hour
           | ) tmp
           | group by tmp.ak,tmp.at,tmp.hour""".stripMargin)
        .createOrReplaceTempView("visit_page_sum_once")
      spark.sql(//|ak，at，所有page的访问次数=1|
        s"""select app_key,hour,sum(one_cp) one_page_count
           | from visit_page_sum_once
           | where one_cp = 1
           | group by app_key,hour""".stripMargin)
        .createOrReplaceTempView("visit_page_once")
    }else {
      spark.sql(//|ak，at，所有page的访问次数=1|
        s"""select tmp.ak app_key,sum(tmp.cp) one_cp from
           | (SELECT ak,at ,COUNT(pp) cp
           | FROM visitor_page
           | GROUP BY ak,at,pp
           | ) tmp
           | group by tmp.ak,tmp.at""".stripMargin)
        .createOrReplaceTempView("visit_page_sum_once")
      spark.sql(//|ak，at，所有page的访问次数=1|
        s"""select app_key,sum(one_cp) one_page_count
           | from visit_page_sum_once
           | where one_cp = 1
           | group by app_key""".stripMargin)
        .createOrReplaceTempView("visit_page_once")
    }

  }

  /**
    * 页面跳出率,每个ak的跳出页个数/总的页面访问量
    */
  def bounceRate(): Unit = {
    if (dimension == "hour") {
      spark.sql(
        """
          |select tpc.app_key app_key,tpc.hour,cast(vpo.one_page_count/tpc.total_page_count as float) bounce_rate
          |from total_page_count tpc
          |left join visit_page_once vpo
          |on tpc.app_key = vpo.app_key and tpc.hour = vpo.hour
        """.stripMargin).createOrReplaceTempView("bounce_rate")
    }else {
      spark.sql(
        """
          |select tpc.app_key app_key,cast(vpo.one_page_count/tpc.total_page_count as float) bounce_rate
          |from total_page_count tpc
          |left join visit_page_once vpo
          |on tpc.app_key = vpo.app_key
        """.stripMargin).createOrReplaceTempView("bounce_rate")
    }

  }

  def shareCount(): Unit = {
    if (dimension == "hour") {
      spark.sql(
        """SELECT ak app_key,hour,COUNT(path) share_count FROM visitor_event where ct !='fail' and tp='ald_share_status' and path IS not NULL GROUP BY ak,hour
        """).createOrReplaceTempView("share_count")
    }else {
      spark.sql(
        """SELECT ak app_key,COUNT(path) share_count FROM visitor_event where ct !='fail' and tp='ald_share_status' and path IS not NULL GROUP BY ak
        """).createOrReplaceTempView("share_count")
    }

  }

}
