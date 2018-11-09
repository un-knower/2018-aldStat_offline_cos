package aldwxstat

import java.security.MessageDigest
import java.sql.Statement

import aldwxconfig.ConfigurationUtil
import aldwxutils._
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangyanpeng on 2017/8/31.
  *
  * 参数统计（基于事件的）
  */
@deprecated("Use EventParas instead.", "wangtaiyang")
object EventParasDaily {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "zhangyanpeng")
    val yesterday = TimeUtil.processArgs(args)
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .config("spark.sql.shuffle.partitions", 12)
      .getOrCreate()
    //    val rdd = Aldstat_args_tool.analyze_args(args, spark, DBConf.hdfsUrl).filter("ev='event'and tp !='ald_error_message' and tp !='ald_reachbottom' and tp !='ald_share_click' and tp !='ald_share_status' and tp !='ald_share_user' and tp !='ald_share_chain' and tp !='ald_pulldownrefresh' and tp !='js_err' and ak!='d6be9d076493c2c3f51b0dbf1ac5cab0'").cache()
    val rdd = ArgsTool.getLogs(args, spark, ConfigurationUtil.getProperty("tongji.parquet")).filter("ev='event'and tp !='ald_error_message' and tp !='ald_reachbottom' and tp !='ald_share_click' and tp !='ald_share_status' and tp !='ald_share_user' and tp !='ald_share_chain' and tp !='ald_pulldownrefresh' and tp !='js_err' and ak!='d6be9d076493c2c3f51b0dbf1ac5cab0'").cache()

    var map: mutable.Map[String, String] = scala.collection.mutable.Map()
    val jSONRDD = rdd.toJSON.rdd.map(f = line => {
      map.clear()
      val jsonLine = JSON.parseObject(JSON.parseObject(line.toString).toString)
      var str: String = null

      if (jsonLine.get("ak") != null && jsonLine.get("tp") != null) {
        val ak = jsonLine.get("ak").toString
        val tp = jsonLine.get("tp").toString
        var a = ""
        var b = ""
        if (jsonLine.get("ct") != null) {
          val ct = jsonLine.get("ct").toString
          if (ct.substring(0, 1).equals("{")) {
            if (!ct.toString.equals("{}")) {
              val ctString = ct.toString.substring(1, ct.toString.length - 1)
              val splitComma = ctString.split(",")
              //"ct":"{'distributorsID':'zilin','scene':1007}
              for (i <- 0 until (splitComma.size)) {
                val splitColon = splitComma(i).split(":")
                if (splitColon.size > 1) {
                  //取出事件的名称
                  if (splitColon(0).contains("\'")) {
                    a = splitColon(0).substring(1, splitColon(0).length - 1)
                  } else {
                    a = splitColon(0)
                  }
                  //取出事件的值
                  if (splitColon(1).contains("\'")) {
                    b = splitColon(1).substring(1, splitColon(1).length - 1)
                  } else {
                    b = splitColon(1)
                  }
                  //事件key 和 value 通过 : 拼接
                  str = a + ":" + b
                  //放到map中 通过 "."来拼接key
                  map.put(ak + "." + tp + "." + str, "0")
                } else {
                  //去除 冒号
                  if (splitComma(0).contains("\'")) {
                    a = splitComma(0).substring(1, splitComma(0).length - 1)
                  } else {
                    a = splitComma(0)
                  }
                  if (splitComma(1).contains("\'")) {
                    b = splitComma(1).substring(1, splitComma(1).length - 1)
                  } else {
                    b = splitComma(1)
                  }

                  str = a + ":" + b
                  map.put(ak + "." + tp + "." + str, "0")
                }
              }
            } else {

              map.put(ak + "." + tp + "." + "ct:value_null", "0")
            }

          } else {
            //处理不包括{的 ct
            val a = "ct" + ":" + jsonLine.get("ct")
            map.put(ak + "." + tp + "." + a, "0")
          }
        }
      }


      map.keys.toString().substring(4, map.keys.toString().length - 1)
    }).flatMap(_.split(", "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .filter(!_._1.contains("ct:null"))
      .distinct()

    val rsRDD = jSONRDD.map(x => {
      val a = x._1.split("\\.")
      val b = a(2).split(":")
      if (b.length >= 2) {
        Row(a(0), a(1), (a(0) + a(1)), b(0), b(1), x._2)
      } else {
        Row(a(0), a(1), (a(0) + a(1)), b(0), "value_null", x._2)
      }
    })
    //创建表结构
    val schema = StructType(List(
      StructField("ak", StringType, true),
      StructField("tp", StringType, true),
      StructField("ss", StringType, true),
      StructField("ct_key", StringType, true),
      StructField("ct_value", StringType, true),
      StructField("ct_count", IntegerType, true)
    ))

    //将数据和元信息匹配起来
    val oneDF = spark.createDataFrame(rsRDD, schema)
    oneDF.createTempView("result")

    spark.udf.register("udf", ((s: String) => toHex(MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")))))

    def toHex(bytes: Array[Byte]): String = bytes.map("%02x".format(_)).mkString("")


    val result: DataFrame = spark.sql("select ak,tp,udf(ss),ct_key,ct_value,ct_count,now() from result")

    var statement: Statement = null
    result.foreachPartition((row: Iterator[Row]) => {
      //      val conn = MySqlPool.getJdbcConn()
      //      statement = conn.createStatement()
      //      conn.setAutoCommit(false)
      //      row.foreach(r => {
      //        try {
      //          val app_key = r(0)
      //          val day = yesterday
      //          val ev_id = r(1)
      //          val event_key = r(2)
      //          val ev_paras_name = r(3)
      //          val ev_paras_value = r(4)
      //          val ev_paras_count = r(5)
      //          val update_at = r(6)
      //          val sql = "insert into aldstat_event_paras (app_key,day,ev_id,event_key,ev_paras_name,ev_paras_value,ev_paras_count,update_at)" + s"values ('${app_key}','${day}','${ev_id}','${event_key}','${ev_paras_name}','${ev_paras_value}','${ev_paras_count}','${update_at}') ON DUPLICATE KEY UPDATE ev_paras_count='${ev_paras_count}',update_at='${update_at}'"
      //          statement.addBatch(sql)
      //          statement.executeBatch
      //          conn.commit()
      //        } catch {
      //          case e: Exception => e.printStackTrace()
      //          case _:Exception=>println("1入库出现异常了---",println(r.toString()))
      //            conn.close()
      //        }
      //      })
//      val conn = JdbcUtil.getConn()
//      val statement = conn.createStatement
      val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
      val sqlText = "insert into aldstat_event_paras (app_key,day,ev_id,event_key,ev_paras_name,ev_paras_value,ev_paras_count,update_at)" + s"values (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE ev_paras_count=?,update_at=?"

      row.foreach(r => {
        val app_key = r.get(0)
        val day = yesterday
        val ev_id = r.get(1)
        val event_key = r.get(2)
        val ev_paras_name = r.get(3)
        val ev_paras_value = r.get(4)
        val ev_paras_count = r.get(5)
        val update_at = r.get(6)

        params.+=(Array[Any](app_key, day, ev_id, event_key, ev_paras_name, ev_paras_value, ev_paras_count, update_at, ev_paras_count, update_at))

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
