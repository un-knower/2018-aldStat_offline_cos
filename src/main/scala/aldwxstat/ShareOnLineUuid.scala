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
  * aldwxstat.Share_New_All
  * Created by 18510 on 2017/8/2.
  */
object ShareOnLineUuid {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "weilongsheng")
    val sparkseesion = SparkSession
      .builder()
      //      .master("local")
      .config("spark.sql.shuffle.partitions", 200)
      .appName(this.getClass.getName)
      .getOrCreate()
    val args_limit_tmp = 1000
    val yesterday = TimeUtil.processArgs(args)
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    //    val df_tmp = Aldstat_args_tool.analyze_args(args, sparkseesion, DBConf.hdfsUrl)
    val df_tmp = ArgsTool.getLogs(args, sparkseesion, ConfigurationUtil.getProperty("tongji.parquet"))

    val rdd = df_tmp.toJSON.rdd.map(line => {
      val jsonLine = JSON.parseObject(line)
      val uu = jsonLine.get("uu")
      val path = jsonLine.get("path")
      val ak = jsonLine.get("ak")
      val src = jsonLine.get("wsr_query_ald_share_src")
      //初始化值，判断src是否为空，若不为空则去最后一个值
      var src1 = ""
      if (src != null) {
        src1 = src.toString.split("\\,")(src.toString.split("\\,").length - 1)
      }
      val tp = jsonLine.get("tp")
      val ct = jsonLine.get("ct")
      val ifo = jsonLine.get("ifo")
      val ev = jsonLine.get("ev")
      val hour = jsonLine.get("hour")
      val at = jsonLine.get("at")
      Row(uu, path, ak, src1, tp, ct, ifo, ev, hour, at)
    })
    val schema = StructType(List(
      StructField("uu", StringType, true),
      StructField("path", StringType, true),
      StructField("ak", StringType, true),
      StructField("src1", StringType, true),
      StructField("tp", StringType, true),
      StructField("ct", StringType, true),
      StructField("ifo", StringType, true),
      StructField("ev", StringType, true),
      StructField("hour", StringType, true),
      StructField("at", StringType, true)
    )
    )
    val df = sparkseesion.createDataFrame(rdd, schema)
    //创建临时表
    df.createTempView("share")
    sparkseesion.sqlContext.cacheTable("share")
    //    val share_day = sparkseesion.sql("select * from share where src1 != '' and tp='ald_share_status'")
    //分享次数
    val pageuu_day = sparkseesion.sql("SELECT ak app_key,path page_uri,uu sharer_uuid,COUNT(path) share_count FROM share WHERE ev='event' and ct !='fail' and tp='ald_share_status' and path IS not NULL GROUP BY ak,path,uu")
    //    pageuu.show()
    //统计出分享带来的新增人数
    val persionclick_day = sparkseesion.sql("SELECT ak app_key,src1 sharer_uuid,COUNT(uu) new_count,path page_uri FROM share WHERE ev='event' and path IS not NULL AND tp='ald_share_click' and uu in (select uu from share where ev='app' and ifo='true') GROUP BY ak,src1,path").filter("sharer_uuid != ''")
    persionclick_day.createTempView("persionclick1_day")
    //            persionclick.show()
    pageuu_day.createTempView("pageuu1_day")

    //分享打开次数/分享打开人数
    val share_open_and_user_day = sparkseesion.sql("SELECT ak app_key,path page_uri,src1 sharer_uuid,COUNT(DISTINCT at) share_open_count,count(DISTINCT uu) share_open_user_count FROM share WHERE ev='event' and tp='ald_share_click' and path IS not NULL GROUP BY ak,path,src1")
    share_open_and_user_day.createTempView("share_open_and_user_day")
    //        share_open_and_user.show()
    //回流量
    val share_reflux_ratio_day = sparkseesion.sql("SELECT a.app_key app_key,a.page_uri page_uri,a.sharer_uuid sharer_uuid,ROUND(cast(b.share_open_count/a.share_count as float),2) share_reflux_ratio from pageuu1_day a left join share_open_and_user_day b on a.app_key = b.app_key and a.page_uri = b.page_uri and a.sharer_uuid = b.sharer_uuid")
    share_reflux_ratio_day.createTempView("share_reflux_ratio1_day")

    val new_and_count_day = sparkseesion.sql("(select a.app_key,a.page_uri,a.sharer_uuid,a.share_count,b.new_count from pageuu1_day a left join persionclick1_day b on a.app_key = b.app_key and a.page_uri = b.page_uri and a.sharer_uuid = b.sharer_uuid) union (select b.app_key,b.page_uri,b.sharer_uuid,a.share_count,b.new_count from persionclick1_day b left join pageuu1_day a on a.app_key = b.app_key and a.page_uri = b.page_uri and a.sharer_uuid = b.sharer_uuid)")

    new_and_count_day.createTempView("new_and_count_day")

    val open_and_user_day = sparkseesion.sql("(select a.app_key,a.page_uri,a.sharer_uuid,a.share_open_count,a.share_open_user_count,b.share_reflux_ratio from share_open_and_user_day a left join share_reflux_ratio1_day b on a.app_key = b.app_key and a.page_uri = b.page_uri and a.sharer_uuid = b.sharer_uuid) union (select b.app_key,b.page_uri,b.sharer_uuid,a.share_open_count,a.share_open_user_count,b.share_reflux_ratio from share_reflux_ratio1_day b left join share_open_and_user_day a on a.app_key = b.app_key and a.page_uri = b.page_uri and a.sharer_uuid = b.sharer_uuid)")

    open_and_user_day.createTempView("open_and_user_day")

    val all_new_result_day = sparkseesion.sql("(select " + yesterday + " day, a.app_key,b.page_uri,b.sharer_uuid,a.share_count,a.new_count,b.share_open_count,b.share_open_user_count,b.share_reflux_ratio from new_and_count_day a left join open_and_user_day b on a.app_key = b.app_key and a.page_uri = b.page_uri and a.sharer_uuid = b.sharer_uuid) union (select " + yesterday + " day,b.app_key,b.page_uri,b.sharer_uuid,a.share_count,a.new_count,b.share_open_count,b.share_open_user_count,b.share_reflux_ratio from open_and_user_day b left join new_and_count_day a on a.app_key = b.app_key and a.page_uri = b.page_uri and a.sharer_uuid = b.sharer_uuid)").na.fill(0)
    //all_new_result_day.show()
    //    结果
    //    val result_sum_day = sparkseesion.sql("select " + yesterday + s" day,a.app_key,a.page_uri page_uri,a.sharer_uuid sharer_uuid,b.share_open_user_count,b.share_open_count,c.share_reflux_ratio,a.share_count,d.new_count,UNIX_TIMESTAMP(now()) update_at from pageuu1_day a left join share_open_and_user_day b on a.app_key = b.app_key and a.page_uri = b.page_uri and a.sharer_uuid = b.sharer_uuid left join share_reflux_ratio1_day c on a.app_key = c.app_key and a.page_uri = c.page_uri and a.sharer_uuid = c.sharer_uuid left join persionclick1_day d on a.app_key = d.app_key and a.page_uri =d.page_uri and a.sharer_uuid = d.sharer_uuid").na.fill(0)

    all_new_result_day.createTempView("result_tables_day")

    val user_data_share = sparkseesion.sql("select day,app_key,sharer_uuid,sum(share_open_user_count) share_open_user_count,sum(share_open_count) share_open_count,ROUND(cast(sum(share_open_count)/sum(share_count) as float),2) share_reflux_ratio,sum(share_count) share_count,sum(new_count) new_count from result_tables_day group by app_key,day,sharer_uuid").na.fill(0)
    //        user_data_share.show()
    user_data_share.createTempView("user_tables")

    /**
      * SELECT  id,age,uname FROM demo a WHERE (SELECT count(*) FROM demo b WHERE            * b.uname=a.uname AND b.id>a.id)<3;
      * 等同于spark sql里自带的row_number
      */

    //    判断是否有参数传递，默认将所有uuid的都写入mysql中
    if (args_limit_tmp == 0) {
      user_data_share.foreachPartition(line => {
        dataframe2mysqlUser(line)
      })
    } else {
      val tmp = sparkseesion.sql(s"select * from (SELECT *, row_number() OVER (PARTITION BY app_key ORDER BY share_count DESC) rank FROM user_tables) where rank <= ${args_limit_tmp}")
      tmp.foreachPartition(line => {
        dataframe2mysqlUser(line)
      })
    }
    sparkseesion.close()
  }

  def dataframe2mysqlUser(iterator: Iterator[Row]): Unit = {
    val UpDataTime = new Timestamp(System.currentTimeMillis())

    val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
    val sqlText = s"insert into aldstat_dailyshare_user (day,app_key,sharer_uuid,share_open_user_count,share_open_count,share_reflux_ratio,share_count,new_count,update_at) values (?,?,?,?,?,?,?,?,?) " +
      s"ON DUPLICATE KEY UPDATE share_count=?,new_count=?,update_at=?,share_open_user_count=?,share_open_count=?,share_reflux_ratio=?"

    //conn.setAutoCommit(false)
    iterator.foreach(r => {
      val date = r.get(0)
      val app_key = r.get(1)
      val page_uri = r.get(2)
      val share_open_user_count = r.get(3)
      val share_open_count = r.get(4)
      val share_reflux_ratio = r.get(5)
      val share_count = r.get(6)
      val new_count = r.get(7)
      val update_at = UpDataTime

      params.+=(Array[Any](date, app_key, page_uri, share_open_user_count, share_open_count, share_reflux_ratio, share_count, new_count, update_at
        , share_count, new_count, update_at, share_open_user_count, share_open_count, share_reflux_ratio))
    })
    JdbcUtil.doBatch(sqlText,params)
  }
}
