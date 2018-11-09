package aldwxstat

import java.sql.Timestamp

import aldwxconfig.ConfigurationUtil
import aldwxutils._
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * 关键用户分析
  */
object CritiaclUser {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "weilongsheng")
    val sparkseesion = SparkSession
      .builder()
      .appName(this.getClass.getName)
      //.config("spark.sql.shuffle.partitions", 12)
      .getOrCreate()
    val yesterday = TimeUtil.processArgs(args)
    val dateTime = TimeUtil.getTimestamp(yesterday)
    val UpDataTime = new Timestamp(System.currentTimeMillis())

    val df_tmp = ArgsTool.getLogs(args, sparkseesion, ConfigurationUtil.getProperty("tongji.parquet")).filter("ev = 'event' and tp = 'ald_share_chain'")


    val rdd = df_tmp.toJSON.rdd.map(line => {
      val jsonLine = JSON.parseObject(line)
      val ak = jsonLine.get("ak")
      val chain = jsonLine.get("ct_chain")
      //初始化值，判断src是否为空，若不为空则去最后一个值
      var user1 = ""
      var user2 = ""
      var user3 = ""
      if (chain != null) {
        if (chain.toString.split("\\,").length == 1) {
          user1 = chain.toString.split("\\,")(0)
        } else if (chain.toString.split("\\,").length == 2) {
          user1 = chain.toString.split("\\,")(0)
          user2 = chain.toString.split("\\,")(1)
        } else if (chain.toString.split("\\,").length == 3) {
          user1 = chain.toString.split("\\,")(0)
          user2 = chain.toString.split("\\,")(1)
          user3 = chain.toString.split("\\,")(2)
        }
      }
      val tp = jsonLine.get("tp")
      val ev = jsonLine.get("ev")
      Row(ak, user1, user2, user3, tp, ev)
    })
    val schema = StructType(List(
      StructField("ak", StringType, true),
      StructField("user1", StringType, true),
      StructField("user2", StringType, true),
      StructField("user3", StringType, true),
      StructField("tp", StringType, true),
      StructField("ev", StringType, true)
    )
    )
    val df = sparkseesion.createDataFrame(rdd, schema)

    //创建临时表
    df.registerTempTable("share")
    val share = sparkseesion.sql("select ak,user1,user2,user3 from share").distinct().na.fill("null")
    share.foreachPartition((rows: Iterator[Row]) => {
//      val conn = JdbcUtil.getConn()
//      val statement = conn.createStatement
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = s"insert into aldstat_key_user (app_key,day,share_uuid,secondary_share_uuid,third_share_uuid,update_at)" +
        s"values (?,?,?,?,?,?) ON " +
        s"DUPLICATE KEY UPDATE update_at=?"
      rows.foreach(r => {

        val app_key = r(0)
        //          val  = r(1)
        val share_uuid = r.get(1)
        val secondary_share_uuid = r.get(2)
        val third_share_uuid = r.get(3)
        val day = dateTime
        val update_at = UpDataTime
        //          val type_value = ""
        params.+=(Array[Any](app_key, day, share_uuid, secondary_share_uuid, third_share_uuid, update_at, update_at))

      })
      //        try {
      JdbcUtil.doBatch(sqlText, params) //批量入库
      //        }
      //        finally {
      //          statement.close() //关闭statement
      //          conn.close() //关闭数据库连接
      //        }
    })
    sparkseesion.close()
  }
}
