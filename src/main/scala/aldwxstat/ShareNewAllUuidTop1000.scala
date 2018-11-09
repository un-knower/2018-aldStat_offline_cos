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
object ShareNewAllUuidTop1000 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    AldwxDebug.debug_info("2017-11-06", "weilongsheng")

    //==========================================================1
    /*
    *gcs:
    *创建一个sparkSession对象
    */
    val sparkseesion = SparkSession
      .builder()
      //      .master("local")
      //      .config("spark.sql.shuffle.partitions", 200)
      .appName(this.getClass.getName)
      //.config("spark.sql.small.file.combine", "true")
      .getOrCreate()

    val args_limit_tmp = 10
    val yesterday = TimeUtil.processArgs(args)  //gcs:返回在执行这个jar包的时候的-d 后面的参数
    val UpDataTime = new Timestamp(System.currentTimeMillis()) //gcs:获得当前的时间

    //==========================================================2
    /*
    *gcs:
    *从parquet的Log日志中读取数据
    */
    //    val df_tmp = Aldstat_args_tool.analyze_args(args, sparkseesion, DBConf.hdfsUrl)
    val df_tmp = ArgsTool.getLogs(args, sparkseesion, ConfigurationUtil.getProperty("tongji.parquet"))



//    val df_tmp =ArgsTool.getTencentDailyDataFrame(sparkseesion, ConfigurationUtil.getProperty("tencent.parquet"))

    //==========================================================3
    /*
    *gcs:
    *解析读取到的所有的parquet的文件。
    * 这里采用的思想是将读取到的df_tmp(sql.DataFrame) 转换成为RDD。
    * 之后提取出要提取出来的字段之后，再将提取出来的字段组合成一个Row。
    * 这样最后的RDD的结果就是RDD[Row]了
    */
    val rdd = df_tmp.toJSON.rdd.map(line => {
      val jsonLine = JSON.parseObject(line)
      val uu = jsonLine.get("uu")
      val path = jsonLine.get("path")
      val ak = jsonLine.get("ak")
      val src = jsonLine.get("wsr_query_ald_share_src")
      //初始化值，判断src是否为空，若不为空则去最后一个值
      var src1 = ""
      //==========================================================4
      /*
      *gcs:
      *解析收取到的字段中的所有的src字段
      */
      if (src != null) {
        src1 = src.toString.split("\\,")(src.toString.split("\\,").length - 1)
      }

      //==========================================================5
      /*
      *gcs:
      *获得json当中的各个要提取的字段
      */
      val tp = jsonLine.get("tp")
      val ct = jsonLine.get("ct")
      val ifo = jsonLine.get("ifo")
      val ev = jsonLine.get("ev")
      val hour = jsonLine.get("hour")
      val at = jsonLine.get("at")
      //==========================================================6
      /*
      *gcs:
      *将刚刚提取到的字段组合成一个Row。这样的话，就可以认为的创建出一个RDD[row]了
      */
      Row(uu, path, ak, src1, tp, ct, ifo, ev, hour, at)
    })

    //==========================================================7
    /*
    *gcs:
    *创建一个表的目录。待会儿会将这些刚刚创建出来的RDD[Row]和这个表目录进行拼接
    */
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

    //==========================================================8
    /*
    *gcs:
    *将刚刚创建出来的RDD[Row]和表结构schema进行拼接，这样就可以创建出一个DataFrame了
    */
    val df = sparkseesion.createDataFrame(rdd, schema)

    //==========================================================9
    /*
    *gcs:
    *对刚刚提取出来的df sql.DataFrame 进行sparkSql的操作
    */
    //创建临时表
    df.createTempView("share")

    //==========================================================10
    /*
    *gcs:
    *这个思想很好，将刚刚提取出来的视图存入到cache当中，这样以后在操作的时候，就不用再去生成了
    */
    sparkseesion.sqlContext.cacheTable("share")
    //    val share = sparkseesion.sql("select * from share where")
    //    分享次数,

    //==========================================================11
    /*
    *gcs:
    *执行sparkSql的操作
    */
    /**<br>gcs:<br>
      * 在。。。。。
      * */
    val pageuu = sparkseesion.sql("SELECT ak app_key,hour,count(DISTINCT uu) share_user_count,COUNT(path) share_count FROM share WHERE ev='event' and ct !='fail' and tp='ald_share_status' and path IS not NULL GROUP BY ak,hour")
    //        pageuu.show()
    //统计出分享带来的新增人数
    val persionclick = sparkseesion.sql("SELECT ak app_key,hour,COUNT(DISTINCT uu) new_count FROM share WHERE ev='event' and path IS not NULL AND tp='ald_share_click' and uu in (select uu from share where ev='app' and ifo='true') GROUP BY ak,hour")
    pageuu.createTempView("pageuu1")
    persionclick.createTempView("persionclick1")


    //sparkseesion.sql("select b.app_key,b.hour,a.share_user_count,a.share_count tmp,b.new_count from persionclick1 b left join pageuu1 a on a.app_key = b.app_key and a.hour = b.hour").show(10000)

    val new_and_count = sparkseesion.sql("(select a.app_key,a.hour,a.share_user_count,a.share_count,b.new_count from pageuu1 a left join persionclick1 b on a.app_key = b.app_key and a.hour = b.hour) union (select b.app_key,b.hour,a.share_user_count,a.share_count,b.new_count from persionclick1 b left join pageuu1 a on a.app_key = b.app_key and a.hour = b.hour)")

    //    new_and_count.show()
    new_and_count.createTempView("new_and_count")

    //分享打开次数/分享打开人数
    val share_open_and_user = sparkseesion.sql("SELECT ak app_key,hour,COUNT(DISTINCT at) share_open_count,count(DISTINCT uu) share_open_user_count FROM share WHERE ev='event' and tp='ald_share_click' and path IS not NULL GROUP BY ak,hour")
    share_open_and_user.createTempView("share_open_and_user")
    //回流量
    val share_reflux_ratio = sparkseesion.sql("SELECT a.app_key app_key,a.hour hour,ROUND(cast(b.share_open_count/a.share_count as float),2) share_reflux_ratio from pageuu1 a left join share_open_and_user b on a.app_key = b.app_key " +
      " and a.hour = b.hour")
    share_reflux_ratio.createTempView("share_reflux_ratio1")

    val open_and_user = sparkseesion.sql("(select a.app_key,a.hour,a.share_open_count,a.share_open_user_count,b.share_reflux_ratio from share_open_and_user a left join share_reflux_ratio1 b on a.app_key = b.app_key and a.hour = b.hour) union (select b.app_key,b.hour,a.share_open_count,a.share_open_user_count,b.share_reflux_ratio from share_reflux_ratio1 b left join share_open_and_user a on a.app_key = b.app_key and a.hour = b.hour)")

    open_and_user.createTempView("open_and_user")

    val all_new_result = sparkseesion.sql("(select a.app_key,a.share_user_count,a.share_count,a.new_count,b.share_open_count,b.share_open_user_count,b.share_reflux_ratio,a.hour from new_and_count a left join open_and_user b on a.app_key = b.app_key  and a.hour = b.hour) union (select b.app_key,a.share_user_count,a.share_count,a.new_count,b.share_open_count,b.share_open_user_count,b.share_reflux_ratio,b.hour from open_and_user b left join new_and_count a on a.app_key = b.app_key and a.hour = b.hour)").filter("hour != 'null'").na.fill(0)
    //    all_new_result.show()
    //    结果
    //    val result_sum = sparkseesion.sql("select " + yesterday + s" date,a.app_key,b.share_open_user_count,b.share_open_count,c.share_reflux_ratio,a.share_count,d.new_count,a.share_user_count,a.hour from pageuu1 a left join share_open_and_user b on a.app_key = b.app_key and a.hour = b.hour left join share_reflux_ratio1 c on a.app_key = c.app_key and a.hour = c.hour left join persionclick1 d on a.app_key = d.app_key and a.hour = d.hour").na.fill(0).filter("hour != 0")
    //    result_sum.show()
    //    小时入库
    all_new_result.foreachPartition(rows => {
      dataframe2mysqlHour(rows, yesterday)
    })
    /**
      * --------------------------每天分享---------------------------------
      */
    //    val share_day = sparkseesion.sql("select * from share where src1 != '' and tp='ald_share_status'")
    //分享次数
    val pageuu_day = sparkseesion.sql("SELECT ak app_key,path page_uri,uu sharer_uuid,COUNT(path) share_count FROM share WHERE ev='event' and ct !='fail' and tp='ald_share_status' and path IS not NULL GROUP BY ak,path,uu")
    //    pageuu.show()
    //统计出分享带来的新增人数
    val persionclick_day = sparkseesion.sql("SELECT ak app_key,src1 sharer_uuid,COUNT(DISTINCT uu) new_count,path page_uri FROM share WHERE ev='event' and path IS not NULL AND tp='ald_share_click' and uu in (select uu from share where ev='app' and ifo='true') GROUP BY ak,src1,path").filter("sharer_uuid != ''")
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


    sparkseesion.sqlContext.cacheTable("result_tables_day")

    val all_data_share = sparkseesion.sql("select day,app_key,sum(share_open_user_count),sum(share_open_count),ROUND(cast(sum(share_open_count)/sum(share_count) as float),2),sum(share_count),sum(new_count),count(DISTINCT sharer_uuid) from result_tables_day group by app_key,day").na.fill(0)


    //==========================================================12
    /*
    *gcs:
    *将一个DataFrame插入到数据库当中。
    * line是Row。这里面包含着如何将一个Row的每一个字段都提取出来的过程。
    */
    //    每日的汇总结果入库
    all_data_share.foreachPartition(line => {
      dataframe2mysqlDay(line)
    })

    val page_data_share = sparkseesion.sql("select day,app_key,page_uri,count(DISTINCT sharer_uuid),sum(share_open_user_count),sum(share_open_count),ROUND(cast(sum(share_open_count)/sum(share_count) as float),2),sum(share_count),sum(new_count) from result_tables_day group by app_key,day,page_uri").na.fill(0)

    //        page_data_share.show()
    //    页面的入库
    page_data_share.foreachPartition(line => {
      dataframe2mysqlPage(line)
    })
    val user_data_share = sparkseesion.sql("select day,app_key,sharer_uuid,sum(share_open_user_count) share_open_user_count,sum(share_open_count) share_open_count,ROUND(cast(sum(share_open_count)/sum(share_count) as float),2) share_reflux_ratio,sum(share_count) share_count,sum(new_count) new_count from result_tables_day group by app_key,day,sharer_uuid").na.fill(0)
    //        user_data_share.show()
    user_data_share.createTempView("user_tables")

    /**
      * SELECT  id,age,uname FROM demo a WHERE (SELECT count(*) FROM demo b WHERE            * b.uname=a.uname AND b.id>a.id)<3;
      * 等同于spark sql里自带的row_number
      */

    //    判断是否有参数传递，默认将所有uuid的都写入mysql中
//    if (args_limit_tmp == 0) {
//      user_data_share.foreachPartition(line => {
//        dataframe2mysqlUser(line)
//      })
//    } else {
//      val tmp = sparkseesion.sql(s"select * from (SELECT *, row_number() OVER (PARTITION BY app_key ORDER BY share_count DESC) rank FROM user_tables) where rank <= ${args_limit_tmp}")
//      tmp.foreachPartition(line => {
//        dataframe2mysqlUser(line)
//      })
//    }
    sparkseesion.close()
  }

  def dataframe2mysqlHour(iterator: Iterator[Row], yes: String): Unit = {
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
    val sqlText = s"insert into aldstat_hourly_share_summary (day,app_key,share_open_user_count,share_open_count,share_reflux_ratio,share_count,new_count,update_at,hour,share_user_count) values (?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE share_count=?,new_count=?,update_at=?,share_open_user_count=?,share_open_count=?,share_reflux_ratio=?,share_user_count=?"

    //      conn.setAutoCommit(false)
    iterator.foreach(r => {
      val date = yes
      val app_key = r.get(0)
      val share_user_count = r.get(1)
      val share_count = r.get(2)
      val new_count = r.get(3)
      val share_open_count = r.get(4)
      val share_open_user_count = r.get(5)
      val share_reflux_ratio = r.get(6)
      val hour = r.get(7)
      val update_at = UpDataTime
      params.+=(Array[Any](date, app_key, share_open_user_count, share_open_count, share_reflux_ratio, share_count, new_count, update_at, hour, share_user_count, share_count, new_count, update_at, share_open_user_count, share_open_count, share_reflux_ratio, share_user_count))
    })
    JdbcUtil.doBatch(sqlText, params) //批量入库
  }

  /**<br>gcs:<br>
    * @param iterator 将用于插入数据的Row
    * */
  def dataframe2mysqlDay(iterator: Iterator[Row]): Unit = {
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
    val sqlText = s"insert into aldstat_daily_share_summary (day,app_key,share_open_user_count,share_open_count,share_reflux_ratio,share_count,new_count,update_at,share_user_count) values (?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE share_count=?,new_count=?,update_at=?,share_open_user_count=?,share_open_count=?,share_reflux_ratio=?,share_user_count=?"


    //==========================================================12
    /*
    *gcs:
    *将Row当中的每一个字段都提取出来。之后将从Row中提取出来的字段都存放到一个数组params当中
    */
    //      conn.setAutoCommit(false)aldstat_dailyshare_pag
    iterator.foreach(r => {
      val date = r.get(0)
      val app_key = r.get(1)
      val share_open_user_count = r.get(2)
      val share_open_count = r.get(3)
      val share_reflux_ratio = r.get(4)
      val share_count = r.get(5)
      val new_count = r.get(6)
      val share_user_count = r.get(7)
      val update_at = UpDataTime
      // ON DUPLICATE KEY UPDATE share_count='${share_count}',new_count='${new_count}',update_at='${update_at}',share_open_user_count='${share_open_user_count}',
      // share_open_count='${share_open_count}',share_reflux_ratio='${share_reflux_ratio}',share_user_count='${share_user_count}'"
      params.+=(Array[Any](date, app_key, share_open_user_count, share_open_count, share_reflux_ratio, share_count, new_count, update_at, share_user_count, share_count, new_count, update_at, share_open_user_count, share_open_count, share_reflux_ratio, share_user_count))
    })

    //==========================================================13
    /*
    *gcs:
    *进行批处理插入
    */
    JdbcUtil.doBatch(sqlText, params) //批量入库

  }

  def dataframe2mysqlPage(iterator: Iterator[Row]): Unit = {
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
    val sqlText = s"insert into aldstat_dailyshare_page (day,app_key,page_uri,share_user_count,share_open_user_count,share_open_count,share_reflux_ratio,share_count,new_count,update_at) values (?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE share_count=?,new_count=?,update_at=?,share_open_user_count=?,share_open_count=?,share_reflux_ratio=?,share_user_count=?"

    //      conn.setAutoCommit(false)
    iterator.foreach(r => {
      val date = r(0)
      val app_key = r(1)
      var page_uri=""
      if(r(2).toString.length>255){
        println(r(2).toString)
        page_uri = r(2).toString.substring(0,255)
      }

      val share_user_count = r(3)
      val share_open_user_count = r(4)
      val share_open_count = r(5)
      val share_reflux_ratio = r(6)
      val share_count = r(7)
      val new_count = r(8)
      val update_at = UpDataTime
      params.+=(Array[Any](date, app_key, page_uri, share_user_count, share_open_user_count, share_open_count, share_reflux_ratio, share_count, new_count, update_at, share_count, new_count, update_at, share_open_user_count, share_open_count, share_reflux_ratio, share_user_count))
    })
    JdbcUtil.doBatch(sqlText, params) //批量入库

  }

  def dataframe2mysqlUser(iterator: Iterator[Row]): Unit = {
    val UpDataTime = new Timestamp(System.currentTimeMillis())
    val params = new ArrayBuffer[Array[Any]]() //数组，存储sql模板需要的数据
    val sqlText = s"insert into aldstat_dailyshare_user (day,app_key,sharer_uuid,share_open_user_count,share_open_count,share_reflux_ratio," +
      s"share_count,new_count,update_at) values (?,?,?,?,?,?,?,?,?)" +
      s" ON DUPLICATE KEY UPDATE share_count=?,new_count=?,update_at=?,share_open_user_count=?,share_open_count=?,share_reflux_ratio=?"
    iterator.foreach(r => {
      val date = r(0)
      val app_key = r(1)
      val page_uri = r(2)
      val share_open_user_count = r(3)
      val share_open_count = r(4)
      val share_reflux_ratio = r(5)
      val share_count = r(6)
      val new_count = r(7)
      val update_at = UpDataTime

      params.+=(Array[Any](date, app_key, page_uri, share_open_user_count, share_open_count, share_reflux_ratio, share_count, new_count, update_at, share_count, new_count, update_at, share_open_user_count, share_open_count, share_reflux_ratio))
    })
    JdbcUtil.doBatch(sqlText, params)
  }
}
