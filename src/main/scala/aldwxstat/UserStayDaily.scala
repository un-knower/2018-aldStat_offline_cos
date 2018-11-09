//==========================================================
/*gcs:
*留存率 的7天和30天的数据
*/


package aldwxstat

import aldwxconfig.ConfigurationUtil
import aldwxutils.{JdbcUtil, TimeUtil, _}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by on 2018/1/9.
  */
/**
* <b>author:</b> gcs <br>
* <b>data:</b> 18-5-7 <br>
* <b>description:</b><br>
  *   用户留存率功能模块的7天,30天数据 <br>
* <b>param:</b><br>
* <b>return:</b><br>
*/
@deprecated
object UserStayDaily {
  var records = 0L

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN) //gcs:设定log日志的标准为Warn
    //AldwxDebug.debug_info("2017-11-06", "cuizhangxiu")
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .config("spark.sql.parquet.binaryAsString", "true")
      .getOrCreate()


    val tt = TimeUtil.processArgs(args) //昨天时间yyyyMMdd
    val nowTime = TimeUtil.str2Long(tt) //昨天时间戳
    //获得传入参数的日期 +1 天
    //1-7天的时间戳
    val argTime = nowTime + 86400 * 1000.toLong //1天后的时间戳，截止到昨日凌晨
    val arg7 = argTime - 86400 * 1000 * 8.toLong //7天前的时间戳
    val arg14 = argTime - 86400 * 1000 * 14.toLong //13天前的时间戳
    val arg15 = argTime - 86400 * 1000 * 15.toLong //14天前的时间戳
    val arg30 = argTime - 86400 * 1000 * 30.toLong //29天前的时间戳
    val arg31 = argTime - 86400 * 1000 * 31.toLong //30天前的时间戳


    //创建文件配置对象
    val conf = new Configuration()

    val parquetFiles = ArrayBuffer[String]() //存要读的路径的数组
    val dayFilter = ArrayBuffer[String]()

    //读下面这10天前的数据
    val daysArr = Array(30, 14, 7, 6, 5, 4, 3, 2, 1, 0)


    for (j <- 0 until (daysArr.length)) {
      val jTime = nowTime - (daysArr(j) * TimeUtil.day.toLong)
      val jDay = TimeUtil.long2Str(jTime)
      dayFilter.append(jDay)

      //循环 读取文件

      val path = new Path(ConfigurationUtil.getProperty("tongji.parquet"))
      parquetFiles += (path.toString + s"/$jDay/part-*/")

      //val path = new Path(ConfigurationUtil.getProperty("tencent.parquet"))
      //parquetFiles += (path.toString + s"$jDay*")
    }


    //val reFile1: DataFrame = ArgsTool.getSpecifyTencentDateDF(args, spark, daysArr).filter(col("ev") === "app")
    //读取文件  [今天日期，30天前日期)
    val file: DataFrame = spark.read.option("mergeSchema", "true").parquet(parquetFiles: _*).filter(col("ev") === "app")

    ArgsTool.analysisArgs(args)
    val reFile1 = file.repartition(ArgsTool.partition)

    reFile1.createOrReplaceTempView("demo")


    val res = spark.sql("select ak,uu, st,ifo from demo where st is not null")


    val reFile = res.select(
      reFile1("ak"),
      reFile1("uu"),
      reFile1("st").cast(LongType),
      reFile1("ifo")
    )

    //st字段有脏数据，对脏数据进行过滤
    val df1: DataFrame = reFile.filter(s"st<'$argTime' and st>='$arg7'")
    val df2: DataFrame = reFile.filter(s"st<'$arg14' and st>='$arg15'")
    val df3: DataFrame = reFile.filter(s"st<'$arg30' and st>='$arg31'")
    //把文件中过滤后的所有的数据进行合并
    val everyDayData: DataFrame = df1.union(df2).union(df3)

    //生成预备表table 获得计算用户留存率和新用户留存率的计算数据
    new StayEveryService().dayNumDayStay(spark, everyDayData)

    //通过预备表的数据 获得mysql表中需要的数据 ， app_key day 几天后 新用户数 用户数 新用户留存率 用户留存率
    val rs = spark.sql("select ak,day,time,pleft,npleft,cast(pleft/r_pleft as float),cast(npleft/r_npleft as float) from table")
//    val count = spark.sql("select count(1) from table")
//    records = count.collect()(0).get(0).toString.toLong
//
//    //records = rs.count()
//    import spark.implicits._
//    val rs2: RDD[Row] = rs.map(("", _)).rdd.partitionBy(new UserStayDailyPartitioner).map(_._2)
    //将结果DF传参到写入数据库方法中  添加数据库
    stayForeachPartition(rs)
    //stayForeachPartition2(rs2)
    spark.stop()
  }

  private def stayForeachPartition(stayDf: DataFrame) = {
    // 逐行入库
    stayDf.foreachPartition((rows: Iterator[Row]) => {

      val sqlText = "insert into ald_stay_logs (app_key,day,day_after,new_people_left,people_left,new_people_ratio,active_people_ratio) values (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE new_people_left=?, people_left=?, new_people_ratio=?, active_people_ratio=?"
      val params = new ArrayBuffer[Array[Any]]()

      for (r <- rows) {
        val ak = r.get(0) // 小程序唯一标识
        val day = r.get(1) // 日期
        val diff = r.get(2).toString.toInt // 第几日的留存
        val people_left = r.get(3) //活跃用户
        val new_people_left = r.get(4) //新增用户
        val active_people_ratio = r.get(5) //活跃用户留存比
        val new_people_ratio = r.get(6) //新增用户留存比


        if (diff == 5) {
          params.+=(Array[Any](ak, day, diff, new_people_left, people_left, new_people_ratio, active_people_ratio,
            new_people_left, people_left, active_people_ratio, new_people_ratio))
        } else {
          params.+=(Array[Any](ak, day, diff, new_people_left, people_left, active_people_ratio, new_people_ratio,
            new_people_left, people_left, new_people_ratio, active_people_ratio))
        }
      }

      JdbcUtil.doBatch(sqlText, params)
    })
  }

  private def stayForeachPartition2(stayDf: RDD[Row]): Unit = {
    // 逐行入库
    stayDf.foreachPartition((rows: Iterator[Row]) => {

      val sqlText = "insert into ald_stay_logs (app_key,day,day_after,new_people_left,people_left,new_people_ratio,active_people_ratio) values (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE new_people_left=?, people_left=?, new_people_ratio=?, active_people_ratio=?"
      val params = new ArrayBuffer[Array[Any]]()

      for (r <- rows) {
        val ak = r.get(0) // 小程序唯一标识
        val day = r.get(1) // 日期
        val diff = r.get(2).toString.toInt // 第几日的留存
        val people_left = r.get(3) //活跃用户
        val new_people_left = r.get(4) //新增用户
        val active_people_ratio = r.get(5) //活跃用户留存比
        val new_people_ratio = r.get(6) //新增用户留存比


        if (diff == 5) {
          params.+=(Array[Any](ak, day, diff, new_people_left, people_left, new_people_ratio, active_people_ratio,
            new_people_left, people_left, active_people_ratio, new_people_ratio))
        } else {
          params.+=(Array[Any](ak, day, diff, new_people_left, people_left, active_people_ratio, new_people_ratio,
            new_people_left, people_left, new_people_ratio, active_people_ratio))
        }
      }

      JdbcUtil.doBatch(sqlText, params)
    })
  }
}
