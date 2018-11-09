package aldwxstat.aldwls

import org.apache.spark.sql.SparkSession


/**
* <b>author:</b> wangtaiyang <br>
* <b>data:</b> 2017/12/14 <br>
* <b>description:</b><br>
  *从刚才创建的app_tmp视图中分析各个指标。如:新用户数、访问人数、打开次数 <br>
  *或者从
* <b>param:</b><br>
  *@param spark 进行运算的时候使用的sparkSession
  *@param dimension 标识当前正在执行哪一个维度。dimension可以取我们数组中列出的7个值，"wvv", "nt", "lang", "wv", "sv", "wsdk", "ww_wh"
* <b>return:</b><br>
*/
class PublicModularDetails(spark: SparkSession, dimension: String) extends PublicModularAnalysis {

  //==========================================================1
  /*
  *gcs:
  *使用app_tmp进行计算的指标。也就是使用ev=app 的类型的数据进行计算的指标
  */
  /**
    * 新用户数 <br>
    * 使用app_tmp来进行计算
    */
  def newUser(): Unit = {
    if (dimension == "ww,wh") {
      spark.sql(
        s"""SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(DISTINCT uu) new_comer_count
           | FROM app_tmp
           | WHERE ifo='true'
           | GROUP BY ak,concat(ww,'*',wh)"""
          .stripMargin)
        .createOrReplaceTempView("new_user_daily")
      //spark.sql("select * from new_user_daily").show()
    } else {
      spark.sql(
        s"""SELECT ak app_key,${dimension} tmp_sum,COUNT(DISTINCT uu) new_comer_count
           | FROM app_tmp
           | WHERE ifo='true'
           | GROUP BY ak,${dimension}"""
          .stripMargin)
        .createOrReplaceTempView("new_user_daily")
    }

  }

  /**
    * 访问人数 <br>
    * 使用app_tmp来进行计算
    */
  def visitorCount(): Unit = {
    if (dimension == "ww,wh") {
      spark.sql(
        s"""SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(DISTINCT uu) visitor_count
           |FROM app_tmp
           |GROUP BY ak,concat(ww,'*',wh)"""
          .stripMargin)
        .createOrReplaceTempView("visitor_count")
    } else {
      spark.sql(
        s"""SELECT ak app_key,${dimension} tmp_sum,COUNT(DISTINCT uu) visitor_count
           |FROM app_tmp
           |GROUP BY ak,${dimension}"""
          .stripMargin)
        .createOrReplaceTempView("visitor_count")
    }

  }

  /**
    * 打开次数 <br>
    * 使用app_tmp来进行计算
    */
  def openCount(): Unit = {
    if (dimension == "ww,wh") {
      spark.sql(
        s"""SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(DISTINCT at) open_count
           |FROM app_tmp
           |GROUP BY ak,concat(ww,'*',wh)"""
          .stripMargin)
        .createOrReplaceTempView("open_count")
    }else {
      spark.sql(
        s"""SELECT ak app_key,${dimension} tmp_sum,COUNT(DISTINCT at) open_count
           |FROM app_tmp
           |GROUP BY ak,${dimension}"""
          .stripMargin)
        .createOrReplaceTempView("open_count")
    }

  }

  //==========================================================2
  /*
  *gcs:
  *使用 visitor_page 的视图进行计算的。
  */
  /**
    * 访问次数，页面总访问量 <br>
    *   使用visitor_page 视图来进行计算
    */
  def totalPageCount(): Unit = {
    if (dimension == "ww,wh") {
      spark.sql(
        s"""
           |SELECT ak app_key,concat(ww,'*',wh) tmp_sum,COUNT(pp) total_page_count
           |FROM visitor_page
           |GROUP BY ak,concat(ww,'*',wh)"""
          .stripMargin)
        .createOrReplaceTempView("total_page_count")
    }else {
      /*
      *gcs:
      *这里也要进行更改。在计算跳出率的时候，要使用"同一个ak下的at的page为1的at个数/同一个ak下的distinct(at)"
      */
      spark.sql(
        s"""
           |SELECT ak app_key,${dimension} tmp_sum,COUNT(pp) total_page_count
           |FROM visitor_page
           |GROUP BY ak,${dimension}"""
          .stripMargin)
        .createOrReplaceTempView("total_page_count")
    }

  }

  /**
    * 总停留时长 <br>
    * 使用app_tmp来进行计算
    */
  def totalStayTime(): Unit = {
    if (dimension == "ww,wh") {
      spark.sql(
        s"""SELECT ak app_key,tmp_sum,sum(time) total_stay_time
           |FROM (
           |SELECT ak,concat(ww,'*',wh) tmp_sum,max(dr/1000) time
           |FROM app_tmp GROUP BY ak,at,concat(ww,'*',wh)
           |) group by tmp_sum,ak"""
          .stripMargin)
        .createOrReplaceTempView("total_stay_time")
    }else {
      spark.sql(
        s"""SELECT ak app_key,${dimension} tmp_sum,sum(time) total_stay_time
           |FROM (
           |SELECT ak,${dimension},max(dr/1000) time
           |FROM app_tmp GROUP BY ak,at,${dimension}
           |) group by ${dimension},ak"""
          .stripMargin)
        .createOrReplaceTempView("total_stay_time")
    }
  }

  /**
    * 次均停留时长
    * 从视图open_count 打开次数（使用app_tmp进行计算）和 total_stay_time (总停留时长,使用app_tmp来进行计算)这两个指标的计算结果中，，来进行Join的操作和计算 <br>
    */
  def secondaryAvgStayTime(): Unit = {
    spark.sql(
      """
        |SELECT oc.app_key,oc.tmp_sum,cast(tst.total_stay_time/oc.open_count as float) secondary_avg_stay_time
        |FROM open_count oc
        |left join total_stay_time tst
        |on oc.app_key=tst.app_key and oc.tmp_sum=tst.tmp_sum
      """.stripMargin).createOrReplaceTempView("secondary_avg_stay_time")
  }

  /**
    * 跳出页个数 <br>
    * 使用visitor_page 来进行计算，即page字段
    */
  def visitPageOnce(): Unit = {
    if (dimension == "ww,wh") {
      spark.sql(//|ak，at，所有page的访问次数=1|
        s"""select tmp.ak app_key,tmp.tmp_sum_one tmp_sum,sum(tmp.cp) one_cp from
           | (SELECT ak,at ,concat(ww,'*',wh) tmp_sum_one,COUNT(pp) cp
           | FROM visitor_page
           | GROUP BY ak,at,pp,concat(ww,'*',wh)
           | ) tmp
           | group by tmp.ak,tmp.at,tmp.tmp_sum_one""".stripMargin)
        .createOrReplaceTempView("visit_page_sum_once")
    }else {
      /*
      *gcs:
      *这里需要进行更改。因为按照pp进行分组，所以说，要进行COUNT(distinct(pp))的操作，才可以算出正确的pp的值
      */
      /*
      *gcs:
      *tmp_sum 是{dimension} 维度的类型
      */
      spark.sql(//|ak，at，所有page的访问次数=1|
        s"""select tmp.ak app_key,tmp.tmp_sum_one tmp_sum,sum(tmp.cp) one_cp from
           | (SELECT ak,at ,${dimension} tmp_sum_one,COUNT(pp) cp
           | FROM visitor_page
           | GROUP BY ak,at,pp,${dimension}
           | ) tmp
           | group by tmp.ak,tmp.at,tmp.tmp_sum_one""".stripMargin)
        .createOrReplaceTempView("visit_page_sum_once")
    }
    spark.sql(//|ak，at，所有page的访问次数=1|
      s"""select app_key,tmp_sum,sum(one_cp) one_page_count
         | from visit_page_sum_once
         | where one_cp = 1
         | group by app_key,tmp_sum""".stripMargin)
      .createOrReplaceTempView("visit_page_once")
  }

  /**
    * 页面跳出率,每个ak的跳出页个数/总的页面访问量 <br>
    * 使用 total_page_count （“访问次数”的指标的计算结果，“页面访问次数”是使用visitor_page这个视图来进行计算的），
    * 和使用 visit_page_once （“跳出页个数”这个指标是使用 visitor_page 来计算完成的）这两个视图来进行join的操作
    * open_count 是“打开次数”
    */
  def bounceRate(): Unit = {
    spark.sql(
      """
        |select tpc.app_key app_key,tpc.tmp_sum,cast(vpo.one_page_count/tpc.total_page_count as float) bounce_rate
        |from total_page_count tpc
        |left join visit_page_once vpo
        |on tpc.app_key = vpo.app_key and tpc.tmp_sum=vpo.tmp_sum
      """.stripMargin).createOrReplaceTempView("bounce_rate")
  }

  def shareCount(): Unit = {
  }
}
