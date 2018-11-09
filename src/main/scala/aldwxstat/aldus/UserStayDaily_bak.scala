package aldwxstat.aldus

import java.net.URI
import java.sql.Statement

import aldwxutils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object UserStayDaily_bak {
  def main(args: Array[String]) {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "zhangyanpeng")
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .getOrCreate()

    //获得昨天时间
    val tt= TimeUtil.processArgs(args)
    //获得昨天时间戳
    val nowtime = TimeUtil.str2Long(tt)
    //获得传入参数的日期 +1 天
    //1-7天的时间戳
    val argTime = nowtime+86400*1000.toLong
    val arg7 = argTime-86400*1000*8.toLong
    //14天的时间戳
    val arg15 = argTime-86400*1000*15.toLong
    val arg14 = argTime-86400*1000*14.toLong
    //29天和三十天 之前的时间戳
    val arg31 = argTime-86400*1000*31.toLong
    val arg30 = argTime-86400*1000*30.toLong

    //创建文件配置对象
    val conf = new Configuration()
    val uri =URI.create(s"${DBConf.hdfsPath}")//hdfs的路径
    //获取文件对象
    val fs = FileSystem.get(uri,conf)

    // 读取的文件
    val json_files = ArrayBuffer[String]()
    val day_filter = ArrayBuffer[String]()


    val dayArr=Array(30,14,7,6,5,4,3,2,1,0)
    for (j <- 0 until(dayArr.length)){
      val jTime =  nowtime - (dayArr(j) * TimeUtil.day.toLong)
      val jDay = TimeUtil.long2Str(jTime)
      day_filter.append(jDay)
      //判断最小天数  路径是否存在
      if (fs.isDirectory(new Path(s"${DBConf.hdfsUrl}/$jDay"))){
        //循环 读取文件
        val path = new Path(s"${DBConf.hdfsUrl}/$jDay")
        json_files+= (path.toString + "/*/part-*")
        println(s"${DBConf.hdfsUrl}/$jDay")
      }else{
        println(s"${DBConf.hdfsUrl}/$jDay")
        logger.warn(s"没有最大时间${dayArr(j)} 的路径。。。")
      }
    }

    //读取文件  [今天日期，30天前日期)
    val file: DataFrame =spark.read.parquet(json_files:_*).filter(col("ev")==="app" )
    //重新分区
    val re_file = Aldstat_args_tool.re_partition(file,args)
    val df1: DataFrame =re_file.filter(s"st<'$argTime' and st>='$arg7'")
    val df2: DataFrame =re_file.filter(s"st<'$arg14' and st>='$arg15'")
    val df3: DataFrame =re_file.filter(s"st<'$arg30' and st>='$arg31'")

    val df: DataFrame =df1.union(df2).union(df3)
    //val df: DataFrame =spark.read.json("C:\\Users\\zhangyanpeng\\Desktop\\log1").filter(col("ev")==="app").filter("ak!=''").filter(col("st")>nowtime)

    //新用户人数
    val daily_people_df = df.filter("ifo='true'")
      .select(
        df("ak").alias("base_app_key"),
        from_unixtime((df("st")/1000),"yyyy-MM-dd").alias("base_day"),
        df("uu").alias("base_uuid"),
        df("ifo").alias("base_is_first_come")
      )

    //统计活跃用户 （ifo =‘’）
    val day_after_df = df.select(
      df("ak").alias("app_key"),
      from_unixtime((df("st")/1000),"yyyy-MM-dd").alias("day_after"),
      df("uu").alias("day_after_uuid"),
      df("ifo").alias("day_after_is_first_come")
    ).cache()

    //两个表进行join （笛卡尔积）  新用户
    val _result_df = daily_people_df.crossJoin(
      day_after_df.select(
        "app_key",
        "day_after",
        "day_after_uuid"
      ))

    val _re_df = _result_df.select(
      _result_df("base_app_key"),           //新增ak
      _result_df("base_day"),               //新增day
      _result_df("base_uuid"),              //新增用户uu
      _result_df("base_is_first_come"),     //ifo
      _result_df("day_after"),              //天数
      _result_df("day_after_uuid"),         //活跃用户uu
      datediff(_result_df("day_after"), _result_df("base_day")).alias("diff") //返回两个日期之间的间隔
    ) .filter("base_uuid==day_after_uuid")
      .distinct()

    val _r_df = _re_df.select(
      _re_df("base_app_key"),
      _re_df("base_day"),
      _re_df("diff"),
      when(_re_df("base_is_first_come").isNull, 0).otherwise(1).alias("is_first_come")
    ).groupBy("base_app_key", "base_day", "diff")
      //不分组聚合
      .agg(
      sum("is_first_come") as "new_people_left"
    ).na.fill(0)
      //.withColumn("new_people_left", lit(0)) //拼接到后面的一列
      .filter(column("diff")===30 || column("diff")===14 || column("diff")===7 || column("diff")===6 || column("diff")===5 || column("diff")===4 || column("diff")===3 || column("diff")===2 || column("diff")===1 || column("diff")===0) //过滤



    //-------------------------------------
    //统计时间内的 活跃用户
    val active_people_df =df.select(
      df("ak").alias("active_app_key"),
      from_unixtime((df("st")/1000),"yyyy-MM-dd").alias("active_day"),
      df("uu").alias("active_uuid")
      //df("ifo").alias("active_is_first_come")
    )

    //两个表进行join （笛卡尔积）  统计时间内的 活跃用户
    val active_result_df = active_people_df.crossJoin(
      day_after_df.select(
        "app_key",
        "day_after",
        "day_after_uuid"
      ))

    val active_re_df = active_result_df.select(
      active_result_df("active_app_key"),           //活跃用户ak
      active_result_df("active_day"),               //活跃用户day
      active_result_df("active_uuid"),              //活跃用户uu
      //active_result_df("active_is_first_come"),     //ifo
      active_result_df("day_after"),              //天数
      active_result_df("day_after_uuid"),         //活跃用户uu
      datediff(active_result_df("day_after"), active_result_df("active_day")).alias("diff") //返回两个日期之间的间隔
    ) .filter("active_uuid==day_after_uuid")
      .distinct()


    val active_r_df = active_re_df.select(
      active_re_df("active_app_key"),
      active_re_df("active_day"),
      active_re_df("diff"),
      active_re_df("day_after_uuid")
      //when(active_re_df("active_is_first_come").isNull, 1).otherwise(1).alias("active_come")
    ).groupBy("active_app_key", "active_day", "diff")
      //不分组聚合
      .agg(
      countDistinct("day_after_uuid")  as "people_left"
    ).na.fill(0)
      //.withColumn("new_people_left", lit(0)) //拼接到后面的一列
      .filter(column("diff")===30 || column("diff")===14 || column("diff")===7 || column("diff")===6 || column("diff")===5 || column("diff")===4 || column("diff")===3 || column("diff")===2 || column("diff")===1 || column("diff")===0) //过滤


    /*val rs_df = active_r_df.join(_r_df,active_r_df("active_app_key") === _r_df("base_app_key") &&
      active_r_df("active_day") === _r_df("base_day") &&
      active_r_df("diff") === _r_df("diff")
    ).select(
      active_r_df("active_app_key"),
      active_r_df("active_day"),
      active_r_df("diff"),
      active_r_df("people_left"),
      _r_df("new_people_left")
    )*/

    val rs_df = active_r_df.join(_r_df,active_r_df("active_app_key") === _r_df("base_app_key") &&
      active_r_df("active_day") === _r_df("base_day") &&
      active_r_df("diff") === _r_df("diff")
    ).select(
      active_r_df("active_app_key") alias("ak"),
      active_r_df("active_day") alias("day"),
      active_r_df("diff") alias("time"),
      active_r_df("people_left") alias("pleft"),
      _r_df("new_people_left") alias("npleft")
    )

    val df_1 =rs_df.filter(rs_df("time")===0)
      .select(
        rs_df("ak") alias("r_ak"),
        rs_df("day") alias("r_day"),
        rs_df("npleft") alias("r_npleft"),
        rs_df("pleft") alias("r_pleft")
      )


    df_1.join(rs_df,rs_df("ak")===df_1("r_ak") && rs_df("day") === df_1("r_day"))
      .select(
        rs_df("ak"),
        rs_df("day"),
        rs_df("time"),
        rs_df("pleft"),
        rs_df("npleft"),
        df_1("r_npleft"),
        df_1("r_pleft")
      ).createTempView("table")


    val rs = spark.sql("select ak,day,time,pleft,npleft,cast(pleft/r_pleft as float),cast(npleft/r_npleft as float) from table")

    var statement:Statement=null
    // 写入数据库
    // 逐行入库
    rs.foreachPartition((rows:Iterator[Row]) => {
      //连接
      val conn =MySqlPool.getJdbcConn()
      statement = conn.createStatement()
      try {
        conn.setAutoCommit(false)
        rows.foreach(r => {
          val ak = r(0)
          val day = r(1)
          val diff = r(2)
          val people_left = r(3)        //活跃用户
          val new_people_left = r(4)    //新增用户
          val active_people_ratio=r(5)   //活跃用户留存比
          val new_people_ratio=r(6)      //新增用户留存比

          val sql = s"insert into ald_stay_logs (app_key,day,day_after,new_people_left,people_left,new_people_ratio,active_people_ratio)" +
            s"values ('${ak}', '${day}', '${diff}','${new_people_left}','${people_left}','${new_people_ratio}','${active_people_ratio}') ON DUPLICATE KEY UPDATE new_people_left='${new_people_left}',people_left='${people_left}',new_people_ratio='${new_people_ratio}',active_people_ratio='${active_people_ratio}'"
          statement.addBatch(sql)
        })
        statement.executeBatch
        conn.commit()

      }catch {
        case e: Exception => e.printStackTrace()
          conn.close()
      }
    })

    spark.stop()
  }
}
