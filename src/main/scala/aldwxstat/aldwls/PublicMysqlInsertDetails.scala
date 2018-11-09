package aldwxstat.aldwls

import java.sql.Timestamp

import aldwxutils.{ArgsTool, JdbcUtil}
import org.apache.spark.Partition
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by 18510 on 2018/1/10.
  */
class PublicMysqlInsertDetails(spark: SparkSession) extends PublicMysqlInsertData {

  /**
    * 单个场景值入库函数
    */
  def sceneInsert2db(): Unit = {
    val day = ArgsTool.day
    print(day)
    print("aldstat_scene_statistics")
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val result = spark.sql(
      """
        |select tpc.app_key,tpc.tmp_sum,tpc.total_page_count,vc.visitor_count,oc.open_count,
        |tst.total_stay_time,sast.secondary_avg_stay_time,nu.new_comer_count,vpo.one_page_count,bounce.bounce_rate
        |from total_page_count tpc
        |left join visitor_count vc
        |on vc.app_key=tpc.app_key and vc.tmp_sum=tpc.tmp_sum
        |left join open_count oc
        |on tpc.app_key=oc.app_key and tpc.tmp_sum=oc.tmp_sum
        |left join total_stay_time tst
        |on tpc.app_key=tst.app_key and tpc.tmp_sum=tst.tmp_sum
        |left join secondary_avg_stay_time sast
        |on tpc.app_key=sast.app_key and tpc.tmp_sum=sast.tmp_sum
        |left join new_user_daily nu
        |on tpc.app_key=nu.app_key and tpc.tmp_sum=nu.tmp_sum
        |left join visit_page_once vpo
        |on tpc.app_key=vpo.app_key and tpc.tmp_sum=vpo.tmp_sum
        |left join bounce_rate bounce
        |on tpc.app_key=bounce.app_key and tpc.tmp_sum=bounce.tmp_sum
      """.stripMargin).filter("tmp_sum != '' and tmp_sum != 'null'").na.fill(0)

    //result.show()
    println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    val resultPart: Dataset[Row] = result
    val partitions: Array[Partition] = resultPart.rdd.partitions
    val numPartitions = resultPart.rdd.getNumPartitions
    println(s"result的分区数：$numPartitions,分区数组：${partitions.toString}")
    resultPart.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = s"insert into aldstat_scene_statistics (update_at,app_key,scene_id,scene_page_count,secondary_stay_time,bounce_rate,scene_visitor_count,scene_open_count,total_stay_time,scene_newer_for_app,one_page_count,day)" +
        s"values (?,?,?,?,?,?,?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE update_at=?,scene_page_count=?,secondary_stay_time=?,bounce_rate=?,scene_visitor_count=?,scene_open_count=?,total_stay_time=?,scene_newer_for_app=?,one_page_count=?"

      rows.foreach(r => {
        val update_at = (System.currentTimeMillis() / 1000).toInt
        val app_key = r.get(0)
        val scene_id = r.get(1)
        val scene_page_count = r.get(2)
        val scene_visitor_count = r.get(3)
        val scene_open_count = r.get(4)
        val total_stay_time = r.get(5)
        val secondary_stay_time = r.get(6)
        val scene_newer_for_app = r.get(7)
        val one_page_count = r.get(8)
        val bounce_rate = r.get(9)

        params.+=(Array[Any](update_at, app_key, scene_id, scene_page_count, secondary_stay_time, bounce_rate, scene_visitor_count, scene_open_count, total_stay_time, scene_newer_for_app, one_page_count, day,
          update_at, scene_page_count, secondary_stay_time, bounce_rate, scene_visitor_count, scene_open_count, total_stay_time, scene_newer_for_app, one_page_count))

      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })
  }

  /**
    * 场景值组入库的函数
    */
  def sceneGroupInsert2db(): Unit = {
    val day = ArgsTool.day
    print(day)
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val result = spark.sql(
      """
        |select tpc.app_key,tpc.tmp_sum,tpc.total_page_count,vc.visitor_count,oc.open_count,
        |tst.total_stay_time,sast.secondary_avg_stay_time,nu.new_comer_count,vpo.one_page_count,bounce.bounce_rate
        |from total_page_count tpc
        |left join visitor_count vc
        |on vc.app_key=tpc.app_key and vc.tmp_sum=tpc.tmp_sum
        |left join open_count oc
        |on tpc.app_key=oc.app_key and tpc.tmp_sum=oc.tmp_sum
        |left join total_stay_time tst
        |on tpc.app_key=tst.app_key and tpc.tmp_sum=tst.tmp_sum
        |left join secondary_avg_stay_time sast
        |on tpc.app_key=sast.app_key and tpc.tmp_sum=sast.tmp_sum
        |left join new_user_daily nu
        |on tpc.app_key=nu.app_key and tpc.tmp_sum=nu.tmp_sum
        |left join visit_page_once vpo
        |on tpc.app_key=vpo.app_key and tpc.tmp_sum=vpo.tmp_sum
        |left join bounce_rate bounce
        |on tpc.app_key=bounce.app_key and tpc.tmp_sum=bounce.tmp_sum
      """.stripMargin).na.fill(0).filter("tmp_sum != 0")

    //result.show()
    println("bbbbbbbbbbbbbbbbbbb")
    result.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = s"insert into aldstat_daily_scene_group (update_at,app_key,scene_group_id,page_count,secondary_stay_time,bounce_rate,visitor_count,open_count,total_stay_time,new_comer_count,one_page_count,day)" +
        s"values (?,?,?,?,?,?,?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE update_at=?,page_count=?,secondary_stay_time=?,bounce_rate=?,visitor_count=?,open_count=?,total_stay_time=?,new_comer_count=?,one_page_count=?"
      rows.foreach(r => {
        val update_at = UpDataTime
        val app_key = r.get(0)
        val scene_id = r.get(1)
        val scene_page_count = r.get(2)
        val scene_visitor_count = r.get(3)
        val scene_open_count = r.get(4)
        val total_stay_time = r.get(5)
        val secondary_stay_time = r.get(6)
        val scene_newer_for_app = r.get(7)
        val one_page_count = r.get(8)
        val bounce_rate = r.get(9)
        params.+=(Array[Any](update_at, app_key, scene_id, scene_page_count, secondary_stay_time, bounce_rate, scene_visitor_count, scene_open_count, total_stay_time, scene_newer_for_app, one_page_count, day,
          update_at, scene_page_count, secondary_stay_time, bounce_rate, scene_visitor_count, scene_open_count, total_stay_time, scene_newer_for_app, one_page_count))
      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })
  }

  /**
    * 小时的场景值组入库函数
    */
  def hourSceneGroupInsert2db(): Unit = {
    val day = ArgsTool.day
    print(day)
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val result = spark.sql(
      """
        |select tpc.app_key,tpc.tmp_sum,tpc.total_page_count,vc.visitor_count,oc.open_count,
        |tst.total_stay_time,sast.secondary_avg_stay_time,nu.new_comer_count,vpo.one_page_count,bounce.bounce_rate,tpc.hour
        |from total_page_count tpc
        |left join visitor_count vc
        |on vc.app_key=tpc.app_key and vc.tmp_sum=tpc.tmp_sum and vc.hour=tpc.hour
        |left join open_count oc
        |on tpc.app_key=oc.app_key and tpc.tmp_sum=oc.tmp_sum and oc.hour=tpc.hour
        |left join total_stay_time tst
        |on tpc.app_key=tst.app_key and tpc.tmp_sum=tst.tmp_sum and tst.hour=tpc.hour
        |left join secondary_avg_stay_time sast
        |on tpc.app_key=sast.app_key and tpc.tmp_sum=sast.tmp_sum and sast.hour=tpc.hour
        |left join new_user_daily nu
        |on tpc.app_key=nu.app_key and tpc.tmp_sum=nu.tmp_sum and nu.hour=tpc.hour
        |left join visit_page_once vpo
        |on tpc.app_key=vpo.app_key and tpc.tmp_sum=vpo.tmp_sum and vpo.hour=tpc.hour
        |left join bounce_rate bounce
        |on tpc.app_key=bounce.app_key and tpc.tmp_sum=bounce.tmp_sum and bounce.hour=tpc.hour
      """.stripMargin).distinct().na.fill(0).filter("tmp_sum != 0")

    //result.show()
    println("ddddddddddddddddddddd")
    result.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = s"insert into aldstat_hourly_scene_group (update_at,app_key,hour,scene_group_id,page_count,secondary_stay_time,bounce_rate,visitor_count,open_count,total_stay_time,new_comer_count,one_page_count,day)" +
        s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE update_at=?,page_count=?,secondary_stay_time=?,bounce_rate=?,visitor_count=?,open_count=?,total_stay_time=?,new_comer_count=?,one_page_count=?"

      rows.foreach(r => {
        val update_at = UpDataTime
        val app_key = r.get(0)
        val scene_id = r.get(1)
        val scene_page_count = r.get(2)
        val scene_visitor_count = r.get(3)
        val scene_open_count = r.get(4)
        val total_stay_time = r.get(5)
        val secondary_avg_stay_time = r.get(6)
        val scene_newer_for_app = r.get(7)
        val one_page_count = r.get(8)
        val bounce_rate = r.get(9)
        val hour = r.get(10)
        params.+=(Array[Any](update_at, app_key, hour, scene_id, scene_page_count, secondary_avg_stay_time, bounce_rate, scene_visitor_count, scene_open_count, total_stay_time, scene_newer_for_app, one_page_count, day,
          update_at, scene_page_count, secondary_avg_stay_time, bounce_rate, scene_visitor_count, scene_open_count, total_stay_time, scene_newer_for_app, one_page_count))

      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    }
    )
  }

  /**
    * 小时单个场景值的入库函数
    */
  def hourSceneInsert2db(): Unit = {
    val day = ArgsTool.day
    print(day)
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val result = spark.sql(
      """
        |select tpc.app_key,tpc.tmp_sum,tpc.total_page_count,vc.visitor_count,oc.open_count,
        |tst.total_stay_time,sast.secondary_avg_stay_time,nu.new_comer_count,vpo.one_page_count,bounce.bounce_rate,tpc.hour
        |from total_page_count tpc
        |left join visitor_count vc
        |on vc.app_key=tpc.app_key and vc.tmp_sum=tpc.tmp_sum and vc.hour=tpc.hour
        |left join open_count oc
        |on tpc.app_key=oc.app_key and tpc.tmp_sum=oc.tmp_sum and oc.hour=tpc.hour
        |left join total_stay_time tst
        |on tpc.app_key=tst.app_key and tpc.tmp_sum=tst.tmp_sum and tst.hour=tpc.hour
        |left join secondary_avg_stay_time sast
        |on tpc.app_key=sast.app_key and tpc.tmp_sum=sast.tmp_sum and sast.hour=tpc.hour
        |left join new_user_daily nu
        |on tpc.app_key=nu.app_key and tpc.tmp_sum=nu.tmp_sum and nu.hour=tpc.hour
        |left join visit_page_once vpo
        |on tpc.app_key=vpo.app_key and tpc.tmp_sum=vpo.tmp_sum and vpo.hour=tpc.hour
        |left join bounce_rate bounce
        |on tpc.app_key=bounce.app_key and tpc.tmp_sum=bounce.tmp_sum and bounce.hour=tpc.hour
      """.stripMargin).distinct().na.fill(0).filter("tmp_sum != 0")

    //result.show()
    println("cccccccccccccccccccc")
    result.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = s"insert into aldstat_hourly_scene (update_at,app_key,hour,scene_id,page_count,secondary_stay_time,bounce_rate,visitor_count,open_count,total_stay_time,new_comer_count,one_page_count,day)" +
        s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE update_at=?,page_count=?,secondary_stay_time=?,bounce_rate=?,visitor_count=?,open_count=?,total_stay_time=?,new_comer_count=?,one_page_count=?"

      rows.foreach(r => {
        val update_at = UpDataTime
        val app_key = r.get(0)
        val scene_id = r.get(1)
        val scene_page_count = r.get(2)
        val scene_visitor_count = r.get(3)
        val scene_open_count = r.get(4)
        val total_stay_time = r.get(5)
        val secondary_avg_stay_time = r.get(6)
        val scene_newer_for_app = r.get(7)
        val one_page_count = r.get(8)
        val bounce_rate = r.get(9)
        val hour = r.get(10)
        params.+=(Array[Any](update_at, app_key, hour, scene_id, scene_page_count, secondary_avg_stay_time, bounce_rate, scene_visitor_count, scene_open_count, total_stay_time, scene_newer_for_app, one_page_count, day,
          update_at, scene_page_count, secondary_avg_stay_time, bounce_rate, scene_visitor_count, scene_open_count, total_stay_time, scene_newer_for_app, one_page_count))
      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    }
    )
  }

  def trendInsert2db(): Unit = {
    val day = ArgsTool.day
    print(day)
    val update_at = (System.currentTimeMillis() / 1000).toInt
    //==========================================================10
    /*
    *gcs:
    *将每一个指标的计算结果，从刚刚创建完成的视图total_page_count，visitor_count，等进行左连接。将最终的所有的指标聚合在一起，之后再算出结果
    */
    val result = spark.sql(
      """
        |select tpc.app_key,tpc.total_page_count,vc.visitor_count,oc.open_count,
        |tst.total_stay_time,sast.secondary_avg_stay_time,nu.new_comer_count,vpo.one_page_count,bounce.bounce_rate,sc.share_count
        |from total_page_count tpc
        |left join visitor_count vc
        |on vc.app_key=tpc.app_key
        |left join open_count oc
        |on tpc.app_key=oc.app_key
        |left join total_stay_time tst
        |on tpc.app_key=tst.app_key
        |left join secondary_avg_stay_time sast
        |on tpc.app_key=sast.app_key
        |left join new_user_daily nu
        |on tpc.app_key=nu.app_key
        |left join visit_page_once vpo
        |on tpc.app_key=vpo.app_key
        |left join bounce_rate bounce
        |on tpc.app_key=bounce.app_key
        |left join share_count sc
        |on tpc.app_key = sc.app_key
      """.stripMargin).na.fill(0)
    //result.show()

    //==========================================================11
    /*
    *gcs:
    *每天的数据是插入到了 aldstat_trend_analysis 这个表当中去
    * app_key 是小程序的key
    * day 是这条数据的产生的时间
    * new_comer_count 是新用户的数目
    * visitor_count 是访问的人数
    * open_count 是打开次数
    * total_page_count 页面的总次数
    * avg_stay_time
    */
    result.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = s"insert into aldstat_trend_analysis (app_key,day,new_comer_count, visitor_count,open_count,total_page_count,avg_stay_time,total_stay_time,daily_share_count,bounce_rate,update_at,one_page_count)" +
        s"values (?,?,?,?,?,?,?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE new_comer_count=?, visitor_count=?, open_count=?,total_page_count=?, avg_stay_time=?,total_stay_time=?, daily_share_count=?, bounce_rate=?,update_at=?,one_page_count=?"

      rows.foreach(r => {
        val app_key = r.get(0)
        val total_page_count = r.get(1)
        val visitor_count = r.get(2)
        val open_count = r.get(3)
        val total_stay_time = r.get(4)
        val avg_stay_time = r.get(5)
        val new_comer_count = r.get(6)
        val one_page_count = r.get(7)
        val bounce_rate = r.get(8)
        val daily_share_count = r.get(9)

        params.+=(Array[Any](app_key, day, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, total_stay_time, daily_share_count, bounce_rate, update_at, one_page_count, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, total_stay_time, daily_share_count, bounce_rate, update_at, one_page_count))

      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })
  }

  /**<br>gcs:<br>
    * 调用每小时的插入数据的方法，将所有的数据插入到每小时的数据当中
    * */
  def hourTrendInsert2db(): Unit = {
    val day = ArgsTool.day
    print(day)
    val update_at = new Timestamp(System.currentTimeMillis())
    val result = spark.sql(
      """
        |select tpc.app_key,tpc.hour,tpc.total_page_count,vc.visitor_count,oc.open_count,
        |tst.total_stay_time,sast.secondary_avg_stay_time,nu.new_comer_count,vpo.one_page_count,bounce.bounce_rate,sc.share_count
        |from total_page_count tpc
        |left join visitor_count vc
        |on vc.app_key=tpc.app_key and vc.hour=tpc.hour
        |left join open_count oc
        |on tpc.app_key=oc.app_key and tpc.hour=oc.hour
        |left join total_stay_time tst
        |on tpc.app_key=tst.app_key and tpc.hour=tst.hour
        |left join secondary_avg_stay_time sast
        |on tpc.app_key=sast.app_key and tpc.hour=sast.hour
        |left join new_user_daily nu
        |on tpc.app_key=nu.app_key and tpc.hour=nu.hour
        |left join visit_page_once vpo
        |on tpc.app_key=vpo.app_key and tpc.hour=vpo.hour
        |left join bounce_rate bounce
        |on tpc.app_key=bounce.app_key and tpc.hour=bounce.hour
        |left join share_count sc
        |on tpc.app_key = sc.app_key and tpc.hour = sc.hour
      """.stripMargin).na.fill(0)
    //result.show()
    result.foreachPartition((rows: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = s"insert into aldstat_hourly_trend_analysis (app_key,day,new_comer_count, visitor_count,open_count,total_page_count,avg_stay_time,total_stay_time,daily_share_count,bounce_rate,update_at,hour,one_page_count)" +
        s"values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE new_comer_count=?, visitor_count=?, open_count=?,total_page_count=?, avg_stay_time=?, total_stay_time=?, daily_share_count=?, bounce_rate=?,update_at=?,one_page_count=?"

      rows.foreach(r => {
        val app_key = r.get(0)
        val hour = r.get(1)
        val total_page_count = r.get(2)
        val visitor_count = r.get(3)
        val open_count = r.get(4)
        val total_stay_time = r.get(5)
        val avg_stay_time = r.get(6)
        val new_comer_count = r.get(7)
        val one_page_count = r.get(8)
        val bounce_rate = r.get(9)
        val daily_share_count = r.get(10)

        params.+=(Array[Any](app_key, day, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, total_stay_time, daily_share_count, bounce_rate, update_at, hour, one_page_count, new_comer_count, visitor_count, open_count, total_page_count, avg_stay_time, total_stay_time, daily_share_count, bounce_rate, update_at, one_page_count))
      })
      JdbcUtil.doBatch(sqlText, params) //批量入库
    })
  }
}
