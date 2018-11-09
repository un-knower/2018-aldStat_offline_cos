//==========================================================
/*gcs:
*事件分析管理 昨日列表，已经弃用
*/

package aldwxstat

import java.security.MessageDigest
import java.sql.Connection
import java.text.SimpleDateFormat

import aldwxconfig.ConfigurationUtil
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import aldwxutils._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangyanpeng on 2017/8/28.
  *
  */
@deprecated("Use Event instead.", "wangtaiyang")
object EventETL {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", 12)
      .appName(this.getClass.getName)
      .getOrCreate()

    //创建对象
    val processArgs = TimeUtil.processArgs2(args)
    val paquter = s"${ConfigurationUtil.getProperty("hdfsUrl")}/${processArgs}/*/part-*" //(测试环境)
    val path = s"${ConfigurationUtil.getProperty("hdfsEtl")}" + processArgs //生产

    //==========================================================1
    /*
    *gcs:
    *从原始数据中把parquet数据读取出来
    */
    val DF = spark.read.parquet(paquter).filter(col("ev") === "event").repartition(100)

    //==========================================================2
    /*
    *gcs:
    *将rdd转换为df进行操作
    */
    val rdd = DF.toJSON.rdd.map(
      line => {
        val jsonLine = JSON.parseObject(line)
        val ak: String = jsonLine.get("ak").toString
        val tp: String = jsonLine.get("tp").toString
        val st = jsonLine.get("st").toString
        val ss = ak + tp
        Row(ak, tp, st, ss)
      }
    ).distinct()

    //==========================================================3
    /*
    *gcs:
    *创建一个df的schema
    */
    //指定dataframe的类型
    val schema = StructType(
      List(
        StructField("ak", StringType, true),
        StructField("tp", StringType, true),
        StructField("st", StringType, true),
        StructField("ss", StringType, true)
      )
    )

    //==========================================================4
    /*
    *gcs:
    *将这个df与schema进行结合，创造出一个DataFrame
    */
    //初始化dataframe
    val df_1 = spark.createDataFrame(rdd, schema)
    df_1.createTempView("tmp")


    //==========================================================5
    /*
    *gcs:
    *注册一个自定义UDF函数
    */
    //udf  函数  (将 tp 和 ak 加密)
    spark.udf.register("udf", ((s: String) => toHex(MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")))))

    def toHex(bytes: Array[Byte]): String = bytes.map("%02x".format(_)).mkString("")


    spark.udf.register("time2date", (s: String) => {
      val regex = """([0-9]+)""".r
      //将时间 切成 11 位的int 类型
      val stimeResult = s.substring(0, s.length - 3) match {
        case regex(num) => num
        case _ => "0"
      }
      val stime = Integer.parseInt(stimeResult)
      val stimeLong: Long = stime.toLong * 1000;
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      sdf.format(stimeLong)
    })


    //==========================================================6
    /*
    *gcs:
    *对dataset进行运算
    */
    val resultDataset: Dataset[Row] = spark.sql("select ak,tp,udf(ss),time2date(st) from tmp").distinct()


    //==========================================================7
    /*
    *gcs:
    *将数据插入到MySql当中
    */
    //假如到mysql 表中
    var conn: Connection = null
    resultDataset.foreachPartition((rows: Iterator[Row]) => {
      //      val conn = MySqlPool.getJdbcConn()
      //      conn.setAutoCommit(false)
      //      val statement = conn.createStatement
      //      try {
      //        conn.setAutoCommit(false)
      //        rows.foreach(r => {
      //          val ak = r(0)
      //          val ev_id = r(1)
      //          val event_key = r(2)
      //          var ev_name = ""
      //          r(1) match {
      //            case "ald_reachbottom" => ev_name = "页面触底(阿拉丁默认)"
      //            case "ald_pulldownrefresh" => ev_name = "下拉刷新(阿拉丁默认)"
      //            case _ => ev_name = r(1).toString
      //          }
      //
      //          val ev_time = r(3)
      //
      //          val sql = s"insert into ald_event (app_key,ev_id,event_key,ev_name,ev_update_time) values ('${ak}','${ev_id}','${event_key}','${ev_name}','${ev_time}') ON DUPLICATE KEY UPDATE ev_update_time='${ev_time}'"
      //          statement.addBatch(sql)
      //        })
      //        statement.executeBatch
      //        conn.commit()
      //      } catch {
      //        case e: Exception => e.printStackTrace()
      //          conn.close()
      //      }
      //val conn = JdbcUtil.getConn()
      //      val statement = conn.createStatement
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

        params.+=(Array[Any](ak, ev_id, event_key, ev_name, ev_time, ev_time))

      })
      //      try {
      JdbcUtil.doBatch(sqlText, params) //批量入库
      //      }
      //      finally {
      //        statement.close() //关闭statement
      //        conn.close() //关闭数据库连接
      //      }
    })

    spark.stop()
  }
}
