package aldwxstat

import java.security.MessageDigest
import java.sql.Connection
import java.text.SimpleDateFormat

import aldwxconfig.ConfigurationUtil
import aldwxutils._
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gaoxiang on 2017/12/15.
  * 过时了
  */
@deprecated("Use Event instead.", "wangtaiyang")
object EventETLHour {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", 12)
      .appName(this.getClass.getName)
      .getOrCreate()

    //获取今天时间
    val today = TimeUtil.processArgsStayTimeDay(args)
    //获取当前小时的前一个小时
    val hour: String = TimeUtil.processArgsStayTimeHour(args)
    //判断是否包含某个小时的文件 需要带的字符串
    val isHour = today + hour
    println("today: " + today.toString)
    println("isHour: " + isHour.toString)
//    println("hdfsPath: " + DBConf.hdfsPath)
//    println("hdfsUrl: " + DBConf.hdfsUrl)
    println("hdfsPath: " + ConfigurationUtil.getProperty("hdfsPath"))
    println("hdfsUrl: " + ConfigurationUtil.getProperty("hdfsUrl"))

    val conf = new Configuration()
//    conf.set("fs.defaultFS", DBConf.hdfsPath)
//    conf.set("fs.defaultFS", DBConf.hdfsPath)
    conf.set("fs.defaultFS", ConfigurationUtil.getProperty("hdfsPath"))
    val fileSystem = FileSystem.get(conf)
    //要读取的日志目录
    val paths: String = ConfigurationUtil.getProperty("hdfsUrl") + "/"
    val fs = fileSystem.listStatus(new Path(paths + today))
    //当前某个小时的所有文件夹
    val listPath = FileUtil.stat2Paths(fs)
    for (readPath <- listPath) {
      //通过for循环处理读判断是否包含要处理小时的文件夹
      if (readPath.toString.contains(s"$isHour")) {
        println(readPath)
        val df = spark.read.parquet(s"$readPath").filter(col("ev") === "event").repartition(100)
        //切分出一个表名要求每次循环里面的名字都不是不一样的
        //hdfs://10.0.0.212:9000/ald_log_parquet/20180402/etl-ald-log-3-2018040220
        val pathnames = readPath.toString.split("/")
        val pathname = pathnames(pathnames.length - 1)
        val tablenames = pathname.split("-")
        val tableOne = tablenames(tablenames.length - 1)
        val tableTwo = tablenames(tablenames.length - 2)
        val tablename = tableOne + tableTwo
        val rdd = df.toJSON.rdd.map(
          line => {
            val jsonLine = JSON.parseObject(line)
            val ak: String = jsonLine.get("ak").toString
            val tp: String = jsonLine.get("tp").toString
            val st = jsonLine.get("st").toString
            val ss = ak + tp
            Row(ak, tp, st, ss)
          }
        ).distinct()
        //指定dataframe的类型
        val schema = StructType(
          List(
            StructField("ak", StringType, true),
            StructField("tp", StringType, true),
            StructField("st", StringType, true),
            StructField("ss", StringType, true)
          )
        )
        val oneDF: DataFrame = spark.createDataFrame(rdd, schema)
        //初始化dataframe

        oneDF.createTempView(s"tmp$tablename")//tmpstart2018040220
        //udf  函数  (将 tp 和 ak 加密)
        spark.udf.register("udf", ((s: String) => toHex(MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")))))

        def toHex(bytes: Array[Byte]): String = bytes.map("%02x".format(_)).mkString("")

        spark.udf.register("time2date", (s: String) => {
          val regex = """([0-9]+)""".r
          //将时间 切成 11 位的int 类型
          val res = s.substring(0, s.length - 3) match {
            case regex(num) => num
            case _ => "0"
          }
          val stime = Integer.parseInt(res)
          val stimeLong: Long = stime.toLong * 1000;
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          sdf.format(stimeLong)
        })

        val result = spark.sql(s"select ak,tp,udf(ss),time2date(st) from tmp$tablename").distinct()

        //入库到mysql 表中
        var conn: Connection = null
        result.foreachPartition((rows: Iterator[Row]) => {
//          val conn = MySqlPool.getJdbcConn()
//          conn.setAutoCommit(false)
//          val statement = conn.createStatement
//          try {
//            conn.setAutoCommit(false)
//            rows.foreach(r => {
//              val ak = r(0)
//              val ev_id = r(1)
//              val event_key = r(2)
//              var ev_name = ""
//              r(1) match {
//                case "ald_reachbottom" => ev_name = "页面触底(阿拉丁默认)"
//                case "ald_pulldownrefresh" => ev_name = "下拉刷新(阿拉丁默认)"
//                case _ => ev_name = r(1).toString
//              }
//              val ev_time = r(3)
//              //如果数据库已经存在ev_id 这里只更新时间
//              val sql = s"insert into ald_event (app_key,ev_id,event_key,ev_name,ev_update_time) values ('${ak}','${ev_id}','${event_key}','${ev_name}','${ev_time}') ON DUPLICATE KEY UPDATE ev_update_time='${ev_time}'"
//              statement.addBatch(sql)
//            })
//            statement.executeBatch
//            conn.commit()
//          } catch {
//            case e: Exception => e.printStackTrace()
//              conn.close()
//          }
//          val conn = JdbcUtil.getConn()
//          val statement = conn.createStatement
          val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
          val sqlText = s"insert into ald_event (app_key,ev_id,event_key,ev_name,ev_update_time) values (?,?,?,?,?) ON DUPLICATE KEY UPDATE ev_update_time=?"

          rows.foreach(r => {
            val ak = r.get(0)
            val ev_id = r.get(1)
            val event_key = r.get(2)
            var ev_name = ""
            r.get(1) match {
              case "ald_reachbottom" => ev_name = "页面触底(阿拉丁默认)"
              case "ald_pulldownrefresh" => ev_name = "下拉刷新(阿拉丁默认)"
              case _ => ev_name = r.get(1).toString
            }
            val ev_time = r.get(3)

            params.+=(Array[Any](ak,ev_id,event_key,ev_name,ev_time,ev_time))

          })
//          try {
            JdbcUtil.doBatch(sqlText, params) //批量入库
//          }
//          finally {
//            statement.close() //关闭statement
//            conn.close() //关闭数据库连接
//          }
        })
      }
    }
    spark.stop()
  }
}
