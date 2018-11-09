package aldwxstat

import java.sql.Statement

import aldwxconfig.ConfigurationUtil
import aldwxutils.{JdbcUtil, TimeUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaoxiang on 2018/1/4.
  */
object StayTime {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .getOrCreate()
    val hour = TimeUtil.processArgsStayTimeHour(args)
    // 获取今天日期
    val dateStr = TimeUtil.processArgsStayTimeDay(args)
    // 当前事件的 unix 时间戳
    val timeStr = TimeUtil.getTimeString()
    allstaytime(spark, hour, dateStr, timeStr)
    spark.close()
  }

  def allstaytime(spark: SparkSession, hour: String, dateStr: String, timeStr: String): Unit = {

    val scene_df = JdbcUtil.readFromMysql(spark, "(select sid, scene_group_id from ald_cms_scene) as scene_df")
    scene_df.createTempView("scene_df")


    /**
      * 通过st计算的读取数据
      */
    val df = spark.read.parquet(s"${ConfigurationUtil.getProperty("tongji.parquet")}/${dateStr}/etl-*${dateStr}${hour}/part-*").filter(" v <= '5.4.1' or v = 'M5.1' or v ='D3.99'").repartition(50)
    df.createTempView("stayTime_hour")

    //读取每天的
    val df_1 = spark.read.parquet(s"${ConfigurationUtil.getProperty("tongji.parquet")}/${dateStr}/*/part-*").filter(" v <= '5.4.1' or v = 'M5.1' or v ='D3.99'").repartition(200)
    df_1.createTempView("stayTime_day")

    /**
      * 通过dr计算的读取数据l
      */
    val df1 = spark.read.parquet(s"${ConfigurationUtil.getProperty("tongji.parquet")}/${dateStr}/etl-*${dateStr}${hour}/part-*").repartition(50)
    df1.createTempView("drstayTime_hour")

    val df2 = spark.read.parquet(s"${ConfigurationUtil.getProperty("tongji.parquet")}/${dateStr}/*/part-*").repartition(200)
    df2.createTempView("drstayTime_day")





    /**
      * 通过st计算的停留时长
      */
    new HourSceneStaytime(spark, dateStr, hour).insert2db() //小时场景值停留时长
    new HourSceneGroupStaytime(spark, dateStr, hour).insert2db() //小时场景值组停留时长
    new HourTrendStaytime(spark, dateStr, hour).insert2db() //小时趋势分析停留时长
    new DaySceneStaytime(spark, dateStr, timeStr).insert2db() //天场景值停留时长
    new DaySceneGroupStaytime(spark, dateStr).insert2db() //天场景值组停留时长
    new DayTrendStaytime(spark, dateStr).insert2db() //天趋势分析停留时长
    /**
      * 通过dr计算的停留时长
      */

    new DRHourSceneStaytime(spark, dateStr, hour).insert2db() //小时场景值停留时长
    new DRHourSceneGroupStaytime(spark, dateStr, hour).insert2db() //小时场景值组停留时长
    new DRHourTrendStaytime(spark, dateStr, hour).insert2db() //小时趋势分析停留时长
    new DRDaySceneStaytime(spark, dateStr, timeStr).insert2db() //天场景值停留时长
    new DRDaySceneGroupStaytime(spark, dateStr).insert2db() //天场景值组停留时长
    new DRDayTrendStaytime(spark, dateStr).insert2db() //天趋势分析停留时长
    spark.close()


  }
}

/**
  * 小时场景值停留时长
  *
  * @param spark
  * @param dateStr
  */
class HourSceneStaytime(spark: SparkSession, dateStr: String, hour: String) extends Serializable {
  def insert2db(): Unit = {

    val sceneopen = spark.sql("SELECT ak, scene, COUNT(DISTINCT at) opencount FROM stayTime_hour WHERE ev = 'app' GROUP BY ak , scene ")
    sceneopen.createTempView("sceneopen")
    //单个场景值的 小时 总停留时长
    spark.sql("SELECT ak,scene,sum(time) total_time FROM " +
      "(SELECT ak,scene,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) time " +
      "FROM stayTime_hour WHERE ev ='page' GROUP BY ak,scene,at) group by ak,scene").distinct().createTempView("scene_hour")
    //单个场景值次均停留时长
    //      val rs_1 = spark.sql("SELECT so.ak,so.scene,sh.total_time ,(sh.total_time/so.opencount) secondary_stay_time FROM sceneopen so LEFT JOIN " +
    //        "scene_hour sh ON so.ak=sh.ak AND so.scene = sh.scene").na.fill(0)
    //
    val rs_1 = spark.sql("SELECT so.ak,so.scene,sh.total_time ,(sh.total_time/so.opencount) secondary_stay_time FROM sceneopen so LEFT JOIN " +
      "scene_hour sh ON so.ak=sh.ak AND so.scene = sh.scene").na.fill(0)
    //rs_1.show()
    var statement: Statement = null
    print("开始跑吧")
    rs_1.foreachPartition((row: Iterator[Row]) => {
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_hourly_scene(`app_key`,`day`,`hour`,`scene_id`,`total_stay_time`,`secondary_stay_time`)" + s" values(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?,secondary_stay_time=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        //          var scene = r.get(1)
        val stay_time = r.get(2)
        val secondary_stay_time = r.get(3)
        //            if(scene == null || scene == "null" || scene == ""){
        //              scene = 11
        //            }
        var scene = 0
        if (r.get(1) == null || r.get(1) == "null" || r.get(1) == "") {
          scene = 11.toString.toInt
        } else {
          scene = r.get(1).toString.toInt
        }
        print(scene, app_key)
        params.+=(Array[Any](`app_key`, `day`, `hour`, `scene`, `stay_time`, `secondary_stay_time`, stay_time, secondary_stay_time))
      })
      JdbcUtil.doBatch(sqlText, params)
    })
  }
}

/**
  * 小时场景
  *
  * @param spark
  * @param dateStr
  * @param hour
  */
class HourSceneGroupStaytime(spark: SparkSession, dateStr: String, hour: String) extends Serializable {

  //外链每日详情入库
  def insert2db(): Unit = {
    //场景值组 小时 总停留时长
    spark.sql("select sh.ak ak ,sh.scene scene ,sd.scene_group_id group_id ,sh.total_time time from scene_hour sh left join scene_df sd on sd.sid=sh.scene").createTempView("group_hour")
    spark.sql("select ak,group_id,sum(time) totaltime from group_hour group by ak,group_id").distinct().createTempView("grouptotlytime")
    //场景值组 小时 打开次数
    spark.sql("SELECT so.ak,so.scene scene ,sd.scene_group_id group_id, " +
      "so.opencount opencountgroup FROM sceneopen so LEFT JOIN " +
      "scene_df sd on sd.sid = so.scene ").createTempView("groupopen")
    spark.sql("SELECT ak,group_id,sum(opencountgroup) opencount FROM groupopen GROUP BY ak,group_id").createTempView("goc")
    val rs_2 = spark.sql("SELECT gt.ak, gt.group_id, gt.totaltime, gt.totaltime/goc.opencount  secondary_stay_time  FROM grouptotlytime gt" +
      " LEFT JOIN goc ON gt.group_id =goc.group_id AND gt.ak =goc.ak").na.fill(0)
    var statement: Statement = null
    //rs_2.show()
    rs_2.foreachPartition((row: Iterator[Row]) => {

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_hourly_scene_group(`app_key`,`day`,`hour`,`scene_group_id`,`total_stay_time`,`secondary_stay_time`)" + s" values(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?,secondary_stay_time=?"
      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        var group_id = r.get(1)
        if (group_id == null) {
          group_id = 11
        }
        val stay_time = r.get(2)
        val secondary_stay_time = r.get(3)

        params.+=(Array[Any](`app_key`, `day`, `hour`, `group_id`, `stay_time`, `secondary_stay_time`, stay_time, secondary_stay_time))

      })
      JdbcUtil.doBatch(sqlText,params)
    })
  }
}

class HourTrendStaytime(spark: SparkSession, dateStr: String, hour: String) extends Serializable {


  def insert2db(): Unit = {

    //趋势分析  小时统计
    spark.sql("SELECT ak,sum(time) total_time FROM (SELECT ak,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) time FROM " +
      "stayTime_hour WHERE ev ='page' GROUP BY ak,at) group by ak").distinct().createTempView("trendtime")
    spark.sql("SELECT ak,COUNT(DISTINCT at) opencount FROM stayTime_hour WHERE ev ='app' GROUP BY ak").createTempView("trendopen")
    val trend_1 = spark.sql("SELECT tt.ak,tt.total_time, tt.total_time/to.opencount secondary_stay_time FROM trendtime tt LEFT JOIN " +
      "trendopen to ON tt.ak = to.ak").na.fill(0)
    var statement: Statement = null
    //trend_1.show()

    trend_1.foreachPartition((row: Iterator[Row]) => {

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_hourly_trend_analysis(`app_key`,`day`,`hour`,`total_stay_time`,`secondary_avg_stay_time`)" + s" values(?,?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?,secondary_avg_stay_time=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        val stay_time = r.get(1)
        val secondary_avg_stay_time = r.get(2)

        params.+=(Array[Any](`app_key`, `day`, `hour`, `stay_time`, `secondary_avg_stay_time`, stay_time, secondary_avg_stay_time))

      })
      JdbcUtil.doBatch(sqlText,params)
    })
  }

}

class DaySceneStaytime(spark: SparkSession, dateStr: String, timeStr: String) extends Serializable {


  def insert2db(): Unit = {
    //单个场景值  天   打开次数
    val sceneopenday = spark.sql("SELECT ak,scene,COUNT(DISTINCT at) opencount FROM stayTime_day WHERE ev = 'app' GROUP BY ak,scene ")
    sceneopenday.createTempView("sceneopen1")
    //单个场景值  天  停留时长
    spark.sql("SELECT ak,scene,sum(time) total_time FROM (SELECT ak,scene,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) time FROM stayTime_day WHERE ev='page' GROUP BY ak,scene,at) group by ak,scene").createTempView("scene_day")
    val rs_3 = spark.sql("SELECT so.ak,so.scene,sh.total_time ,sh.total_time/so.opencount secondary_stay_time FROM sceneopen1 so LEFT JOIN " +
      "scene_day sh ON so.scene =sh.scene AND so.ak = sh.ak").na.fill(0)
    var statement: Statement = null
    //rs_3.show()
    rs_3.foreachPartition((row: Iterator[Row]) => {

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_scene_statistics(`app_key`,`day`,`scene_id`,`total_stay_time`,`secondary_stay_time`,`update_at`)" + s" values(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?,secondary_stay_time=?,update_at=?"
      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        //          var scene = r.get(1)
        val stay_time = r.get(2)
        val secondary_stay_time = r.get(3)
        var scene = 0
        if (r.get(1) == null || r.get(1) == "null" || r.get(1) == "") {
          scene = 11.toString.toInt
        } else {
          scene = r.get(1).toString.toInt
        }
        params.+=(Array[Any](`app_key`, `day`, `scene`, `stay_time`, `secondary_stay_time`, `timeStr`, stay_time, secondary_stay_time, timeStr))

      })
      JdbcUtil.doBatch(sqlText,params)
    })
  }
}

class DaySceneGroupStaytime(spark: SparkSession, dateStr: String) extends Serializable {


  def insert2db(): Unit = {

    //场景值组 tian 总停留时长
    spark.sql("select sh.ak ak ,sh.scene scene ,sd.scene_group_id group_id , " +
      "sh.total_time time from scene_hour sh left join " +
      "scene_df sd on sd.sid=sh.scene").createTempView("group_hour1")
    spark.sql("select ak,group_id,sum(time) time  from group_hour1 group by " +
      "ak,group_id").createTempView("grouptotlytime1")
    //tian 打开次数
    spark.sql("SELECT so.ak,so.scene  ,sd.scene_group_id group_id, " +
      "so.opencount opencountgroup FROM sceneopen1 so LEFT JOIN " +
      "scene_df sd on sd.sid=so.scene ").createTempView("groupopen1")
    //场景值组的次均停留时长
    spark.sql("SELECT ak,group_id,sum(opencountgroup) opencount FROM groupopen1 GROUP BY ak,group_id").createTempView("goc1")
    val rs_4 = spark.sql("SELECT gt.ak, gt.group_id, gt.time, gt.time/goc1.opencount secondary_stay_time FROM grouptotlytime1 gt" +
      " LEFT JOIN goc1  ON gt.group_id =goc1.group_id AND gt.ak =goc1.ak").na.fill(0)
    var statement: Statement = null
    //rs_4.show()
    rs_4.foreachPartition((row: Iterator[Row]) => {

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_daily_scene_group(`app_key`,`day`,`scene_group_id`,`total_stay_time`,`secondary_stay_time`)" +
        s" values(?,?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?,secondary_stay_time=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        var group_id = r.get(1)
        if (group_id == null) {
          group_id = 11
        }

        val stay_time = r.get(2)
        val secondary_stay_time = r.get(3)

        params.+=(Array[Any](`app_key`, `day`, `group_id`, `stay_time`, `secondary_stay_time`, stay_time, secondary_stay_time))

      })
      JdbcUtil.doBatch(sqlText,params)
    })

  }
}

class DayTrendStaytime(spark: SparkSession, dateStr: String) extends Serializable {

  def insert2db(): Unit = {

    //趋势分析   天 统计总停留时长
    spark.sql("SELECT ak, sum(time) total_time, UNIX_TIMESTAMP(now()) FROM (SELECT ak,cast(max(st)/1000 as int)-cast(min(st)/1000 as int) time " +
      "FROM stayTime_day WHERE ev ='page' GROUP BY ak,at) group by ak").distinct().createTempView("trendtime1")
    //趋势分析tian打开次数
    spark.sql("SELECT ak,COUNT(DISTINCT at) opencount FROM stayTime_day WHERE ev ='app' GROUP BY ak").createTempView("trendopen1")
    //趋势分析天 次均停留时长
    val trend_2 = spark.sql("SELECT tt.ak, tt.total_time, tt.total_time/to.opencount secondary_stay_time,UNIX_TIMESTAMP(now())" +
      " FROM trendtime1 tt LEFT JOIN trendopen1 to ON tt.ak = to.ak").na.fill(0)
    var statement: Statement = null
    //trend_2.show()
    trend_2.foreachPartition((row: Iterator[Row]) => {

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_trend_analysis(`app_key`,`day`,`total_stay_time`,`secondary_avg_stay_time`,`update_at`)" + s" values(?,?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?,secondary_avg_stay_time=?,update_at=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        val stay_time = r.get(1)
        val secondary_avg_stay_time = r.get(2)
        val update_at = r.get(3)

        params.+=(Array[Any](`app_key`, `day`, `stay_time`, `secondary_avg_stay_time`, `update_at`, `stay_time`, `secondary_avg_stay_time`, `update_at`))

      })
      JdbcUtil.doBatch(sqlText,params)
    })
  }
}

class DRHourSceneStaytime(spark: SparkSession, dateStr: String, hour: String) extends Serializable {


  def insert2db(): Unit = {
   try{
     val sceneopen = spark.sql("SELECT ak,scene,COUNT(DISTINCT at) opencount FROM drstayTime_hour WHERE ev = 'app' GROUP BY ak,scene ")
     sceneopen.createTempView("drsceneopen")
     //单个场景值的 小时 总停留时长
     spark.sql("SELECT ak,scene,sum(time) total_time FROM (SELECT ak,scene,MAX(dr/1000) time FROM drstayTime_hour WHERE ev ='app' GROUP BY ak,scene,at) group by ak,scene").createTempView("drscene_hour")
     //单个仓净值的次均停留时长
     val rs_1 = spark.sql("SELECT so.ak, so.scene, sh.total_time ,sh.total_time/so.opencount avgtime FROM drsceneopen so LEFT JOIN " +
       "drscene_hour sh ON so.scene =sh.scene AND so.ak = sh.ak").na.fill(0)
     //rs_1.show()
     var statement: Statement = null
     rs_1.foreachPartition((row: Iterator[Row]) => {

       val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
       val sqlText = "insert into aldstat_hourly_scene(`app_key`,`day`,`hour`,`scene_id`,`total_stay_time`,`secondary_stay_time`)" + s" values(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?,secondary_stay_time=?"

       row.foreach(r => {
         val app_key = r.get(0)
         val day = dateStr
         //          var scene = r.get(1)
         val stay_time = r.get(2)
         val secondary_stay_time = r.get(3)
         //          if(scene == null || scene == "null" || scene == ""){
         //            scene = 11
         //          }
         var scene = 0
         if (r.get(1) == null || r.get(1) == "null" || r.get(1) == "") {
           scene = 11.toString.toInt
         } else {
           scene = r.get(1).toString.toInt
         }
         params.+=(Array[Any](`app_key`, `day`, `hour`, `scene`, `stay_time`, `secondary_stay_time`, `stay_time`, `secondary_stay_time`))

       })
       JdbcUtil.doBatch(sqlText,params)
     })
   }catch {
     case e:Exception =>println(e.printStackTrace())
   }

  }
}

class DRHourSceneGroupStaytime(spark: SparkSession, dateStr: String, hour: String) extends Serializable {


  def insert2db(): Unit = {
  try{
    //场景值组 小时 总停留时长
    spark.sql("select sh.ak ak ,sh.scene scene ,sd.scene_group_id group_id , " +
      "sh.total_time time from drscene_hour sh left join " +
      "scene_df sd on sd.sid=sh.scene").createTempView("drgroup_hour")
    spark.sql("select ak,group_id,sum(time) time  from drgroup_hour group by " +
      "ak,group_id").createTempView("drgrouptotlytime")
    //场景值组小时 打开次数
    spark.sql("SELECT so.ak,so.scene scene ,sd.scene_group_id group_id, " +
      "so.opencount opencountgroup FROM drsceneopen so LEFT JOIN " +
      "scene_df sd on sd.sid=so.scene ").createTempView("drgroupopen")
    spark.sql("SELECT ak,group_id,sum(opencountgroup) opencount FROM drgroupopen GROUP BY ak,group_id").createTempView("drgoc")
    //场景值组的次均停留时长
    val rs_2 = spark.sql("SELECT gt.ak,gt.group_id,gt.time,gt.time/drgoc.opencount avgtime FROM drgrouptotlytime gt" +
      " LEFT JOIN drgoc ON gt.group_id =drgoc.group_id AND gt.ak =drgoc.ak").na.fill(0)
    //rs_2.show()
    var statement: Statement = null
    rs_2.foreachPartition((row: Iterator[Row]) => {

      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_hourly_scene_group(`app_key`,`day`,`hour`,`scene_group_id`,`total_stay_time`,`secondary_stay_time`)" + s" values(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?,secondary_stay_time=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = dateStr
        var group_id = r.get(1)
        if (group_id == null) {
          group_id = 11
        }
        val stay_time = r.get(2)
        val secondary_stay_time = r.get(3)

        params.+=(Array[Any](`app_key`, `day`, `hour`, `group_id`, `stay_time`, `secondary_stay_time`, `stay_time`, `secondary_stay_time`))

      })
      JdbcUtil.doBatch(sqlText,params)
    })
  }catch {
    case e:Exception=>print(e.printStackTrace())
  }

  }
}

class DRHourTrendStaytime(spark: SparkSession, dateStr: String, hour: String) extends Serializable {

  def insert2db(): Unit = {
    //趋势分析  总停留时长小时统计
    try{
      spark.sql("SELECT ak,sum(time) total_time FROM (SELECT ak,max(dr/1000) time FROM drstayTime_hour " +
        "WHERE ev ='app' GROUP BY ak,at) group by ak").distinct().createTempView("drtrendtime")
      //趋势分析打开次数
      spark.sql("SELECT ak,COUNT(DISTINCT at) opencount FROM drstayTime_hour WHERE ev ='app' GROUP BY ak").createTempView("drtrendopen")
      val trend_1 = spark.sql("SELECT tt.ak,tt.total_time, tt.total_time/to.opencount avetime FROM drtrendtime tt LEFT JOIN " +
        "drtrendopen to ON tt.ak = to.ak").na.fill(0)
      var statement: Statement = null
      //trend_1.show()
      trend_1.foreachPartition((row: Iterator[Row]) => {

        val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
        val sqlText = "insert into aldstat_hourly_trend_analysis(`app_key`,`day`,`hour`,`total_stay_time`,`secondary_avg_stay_time`)" + s" values(?,?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?,secondary_avg_stay_time=?"

        row.foreach(r => {
          val app_key = r.get(0)
          val day = dateStr
          val stay_time = r.get(1)
          val secondary_avg_stay_time = r.get(2)

          params.+=(Array[Any](`app_key`, `day`, `hour`, `stay_time`, `secondary_avg_stay_time`, `stay_time`, `secondary_avg_stay_time`))

        })
        JdbcUtil.doBatch(sqlText,params)
      })

    }catch {
      case e:Exception=>print(e.printStackTrace())
    }

  }
}

class DRDaySceneStaytime(spark: SparkSession, dateStr: String, timeStr: String) extends Serializable {

  def insert2db(): Unit = {
    try{
      val sceneopenday = spark.sql("SELECT ak,scene,COUNT(DISTINCT at) opencount FROM drstayTime_day WHERE ev = 'app' GROUP BY ak,scene ")
      sceneopenday.createTempView("drsceneopen1")
      //单个场景值的 tian 总停留时长
      spark.sql("SELECT ak,scene,sum(time) total_time FROM (SELECT ak,scene,MAX(dr/1000) time " +
        "FROM drstayTime_day WHERE ev ='app' GROUP BY ak,scene,at) group by ak,scene").createTempView("drscene_day")
      //单个仓净值的次均停留时长
      val rs_3 = spark.sql("SELECT so.ak, so.scene, sh.total_time ,(sh.total_time/so.opencount) secondary_stay_time FROM " +
        " drsceneopen1 so LEFT JOIN drscene_day sh ON so.scene =sh.scene AND so.ak = sh.ak").na.fill(0)
      //rs_3.show()
      var statement: Statement = null
      rs_3.foreachPartition((row: Iterator[Row]) => {

        val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
        val sqlText = "insert into aldstat_scene_statistics(`app_key`,`day`,`scene_id`,`total_stay_time`,`secondary_stay_time`,`update_at`)" + s" values(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?,secondary_stay_time=?,update_at=?"

        row.foreach(r => {
          val app_key = r.get(0)
          val day = dateStr
          //          var scene = r.get(1)
          val stay_time = r.get(2)
          val secondary_stay_time = r.get(3)
          var scene = 0
          if (r.get(1) == null || r.get(1) == "null" || r.get(1) == "") {
            scene = 11.toString.toInt
          } else {
            scene = r.get(1).toString.toInt
          }
          params.+=(Array[Any](`app_key`, `day`, `scene`, `stay_time`, `secondary_stay_time`, `timeStr`, `stay_time`, `secondary_stay_time`, timeStr))

        })
        JdbcUtil.doBatch(sqlText,params)
      })
    }catch {
      case e:Exception=>print(e.printStackTrace())
    }

  }
}

class DRDaySceneGroupStaytime(spark: SparkSession, dateStr: String) extends Serializable {

  def insert2db(): Unit = {
    try{
      //场景值组 tian 总停留时长
      spark.sql("select sh.ak ak ,sh.scene scene ,sd.scene_group_id group_id , " +
        "sh.total_time time from drscene_day sh left join " +
        "scene_df sd on sd.sid=sh.scene").createTempView("drgroup_hour2")
      spark.sql("select ak,group_id,sum(time) time  from drgroup_hour2 group by " +
        "ak,group_id").createTempView("drgrouptotlytime2")
      //tian 打开次数
      spark.sql("SELECT so.ak,so.scene scene ,sd.scene_group_id group_id, " +
        "so.opencount opencountgroup FROM drsceneopen1 so LEFT JOIN " +
        "scene_df sd on sd.sid=so.scene ").createTempView("drgroupopen2")
      //场景值组的次均停留时长
      spark.sql("SELECT ak,group_id,sum(opencountgroup) opencount FROM drgroupopen2 GROUP BY ak,group_id").createTempView("drgoc2")
      val rs_4 = spark.sql("SELECT gt.ak, gt.group_id, gt.time, gt.time/drgoc2.opencount avgtime FROM drgrouptotlytime2 gt" +
        " LEFT JOIN drgoc2  ON gt.group_id =drgoc2.group_id AND gt.ak =drgoc2.ak").na.fill(0)
      //rs_4.show()
      var statement: Statement = null
      rs_4.foreachPartition((row: Iterator[Row]) => {

        val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
        val sqlText = "insert into aldstat_daily_scene_group(`app_key`,`day`,`scene_group_id`,`total_stay_time`,`secondary_stay_time`)" + s" values(?,?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?,secondary_stay_time=?"

        row.foreach(r => {
          val app_key = r.get(0)
          val day = dateStr
          var group_id = r.get(1)
          if (group_id == null) {
            group_id = 11
          }
          val stay_time = r.get(2)
          val secondary_stay_time = r.get(3)

          params.+=(Array[Any](`app_key`, `day`, `group_id`, `stay_time`, `secondary_stay_time`, `stay_time`, `secondary_stay_time`))
        })
        JdbcUtil.doBatch(sqlText,params)
      })
    }catch {
      case e:Exception=>print(e.printStackTrace())
    }

  }
}

class DRDayTrendStaytime(spark: SparkSession, dateStr: String) extends Serializable {

  def insert2db(): Unit = {
    try{
      //趋势分析  总停留时长tian统计
      spark.sql("SELECT ak,sum(time) total_time FROM (SELECT ak,max(dr/1000) time FROM drstayTime_day " +
        "WHERE ev ='app' GROUP BY ak,at) group by ak").distinct().createTempView("drtrendtime2")
      spark.sql("select * from drtrendtime2 where ak='091aeb5a1e3074c1871e9cff846ba657'").show(100)
      //趋势分析tian打开次数
      spark.sql("SELECT ak,COUNT(DISTINCT at) opencount FROM drstayTime_day WHERE ev ='app' GROUP BY ak").createTempView("drtrendopen2")

      val trend_2 = spark.sql("SELECT tt.ak,tt.total_time, tt.total_time/to.opencount avetime,UNIX_TIMESTAMP(now()) FROM drtrendtime2 tt LEFT JOIN " +
        "drtrendopen2 to ON tt.ak = to.ak").na.fill(0)
      var statement: Statement = null
      //trend_2.show()

      trend_2.foreachPartition((row: Iterator[Row]) => {

        val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
        val sqlText = "insert into aldstat_trend_analysis(`app_key`,`day`,`total_stay_time`,`secondary_avg_stay_time`,`update_at`)" + s" values(?,?,?,?,?) ON DUPLICATE KEY UPDATE total_stay_time=?,secondary_avg_stay_time=?,update_at=?"

        row.foreach(r => {
          val app_key = r.get(0)
          val day = dateStr
          val stay_time = r.get(1)
          val secondary_avg_stay_time = r.get(2)
          val update_at = r.get(3)

          if(app_key=="091aeb5a1e3074c1871e9cff846ba657"){
            println(stay_time+"__"+secondary_avg_stay_time)

          }

          params.+=(Array[Any](`app_key`, `day`, `stay_time`, `secondary_avg_stay_time`, `update_at`, `stay_time`, `secondary_avg_stay_time`, `update_at`))

        })
        JdbcUtil.doBatch(sqlText,params)
      })
    }catch {
      case e:Exception=>print(e.printStackTrace())
    }

  }
}



